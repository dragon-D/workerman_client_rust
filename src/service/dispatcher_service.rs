use anyhow::Result;
use std::collections::VecDeque;
use xtra::prelude::{Context, Handler};
use xtra::Actor;

use crate::dispatcher::DispatcherService;
use crate::types::*;

impl Actor for DispatcherService {}

#[async_trait::async_trait]
impl Handler<GatewayConnectionSucess> for DispatcherService {
    async fn handle(
        &mut self,
        message: GatewayConnectionSucess,
        ctx: &mut Context<Self>,
    ) -> Result<()> {
        // self.gateway_managen.do_send_async(message).await?;
        println!("gateway 连接成功: {:?}", 1);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<RegisterClose> for DispatcherService {
    async fn handle(&mut self, message: RegisterClose, ctx: &mut Context<Self>) -> bool {
        match self.gateway_managen.send(message).await {
            Ok(stat) => stat,
            Err(_) => false,
        }
    }
}

#[async_trait::async_trait]
impl Handler<RegisterMessage> for DispatcherService {
    async fn handle(&mut self, message: RegisterMessage, ctx: &mut Context<Self>) -> Result<()> {
        let _ = self.gateway_managen.do_send_async(message).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<GatewayResponse> for DispatcherService {
    async fn handle(&mut self, message: GatewayResponse, ctx: &mut Context<Self>) -> Result<bool> {
        match message.method {
            GatewayMethod::Close => {
                self.req_queue.remove(&message.address);
                let res = GatewayToStatus {
                    address: message.address,
                    opAdd: false,
                };
                let _ = self.gateway_managen.do_send_async(res).await?;
            }
            GatewayMethod::Send => {
                let queue = self.req_queue.get_mut(&message.address).and_then(|queue| {
                    if let Some(task) = queue.pop_front() {
                        // let _ = task.tx.send(message.respond_to).await;
                        return Some(task);
                    };
                    return None;
                });
                if let Some(task) = queue {
                    match task.tx.send((message.address, message.respond_to)).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("queue.pop_back() {:?}, e= {:?}", task, e);
                        }
                    }
                }
            }
            GatewayMethod::Start => {
                self.req_queue
                    .insert(message.address.clone(), VecDeque::new());
            }
            _ => {}
        }
        Ok(true)
    }
}

#[async_trait::async_trait]
impl Handler<ActionMessage> for DispatcherService {
    async fn handle(&mut self, message: ActionMessage, ctx: &mut Context<Self>) -> Result<()> {
        match message {
            ActionMessage::IsOnline { request, tx } => {
                let online = self.is_online(request, tx).await;
            }
            ActionMessage::SendToGroup {
                group,
                message,
                exclude_client_id,
                raw,
            } => {
                match self.send_to_group(group, message, exclude_client_id, raw).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!("send_to_group error, {}", e);
                    }
                }
            }
            ActionMessage::GetUidListGroup { group, tx } => {
                let _ = self.get_uid_list_group(group, tx).await;
            }
            ActionMessage::GetClientIdByUid { uid, tx } => {
                match self.get_client_id_by_uid(uid, tx).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!("is_uid_online error, {}", e);
                    }
                }
            }
            ActionMessage::IsUidOnline { uid, tx } => match self.is_uid_online(uid, tx).await {
                Ok(_) => (),
                Err(e) => {
                    error!("is_uid_online error, {}", e);
                }
            },
            ActionMessage::JoinGroup { client_id, group } => {
                match self.join_group(client_id, group).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!("join_group error, {}", e);
                    }
                }
            }
            ActionMessage::SendToUid { uid, body } => {
                match self.send_to_uid(uid, body).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!("send_to_uid error, {}", e);
                    }
                }
            }
            ActionMessage::LeaveGroup { client_id, group } => {
                match self.leave_group(client_id, group).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!("leave_group error, {}", e);
                    }
                }
            }
            ActionMessage::UnBindUid { client_id, uid } => {}
            ActionMessage::BindUid { client_id, uid } => {
                match self.bind_uid(client_id, uid).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!("bind_uid error, {}", e);
                    }
                }
            }
            _ => {
                error!("##########   没有匹配 ----------- ");
            }
        }
        Ok(())
    }
}
