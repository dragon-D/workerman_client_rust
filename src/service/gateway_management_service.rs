use anyhow::Result;
use iptools::ipv4::{ip2long, long2ip};
use xtra::prelude::{Context, Handler, Message, *};
use xtra::spawn::Tokio as xTokio;
use xtra::Actor;

use crate::gateway_management::GatewayManagement;
use crate::register::RegisterClient;
use crate::types::*;

impl Actor for GatewayManagement {}

#[async_trait::async_trait]
impl Handler<GatewayConnectionSucess> for GatewayManagement {
    async fn handle(
        &mut self,
        msg: GatewayConnectionSucess,
        _ctx: &mut Context<Self>,
    ) -> Result<()> {
        if let Some(client) = self.wait_gateway.remove(&msg.address) {
            //网关连接成功
            self.connect_gateway.insert(msg.address.clone(), client);
        }
        Ok(())
    }
}

/// 注册中心启动
/// 获取网关地址
#[async_trait::async_trait]
impl Handler<GatewayRegisterStart> for GatewayManagement {
    async fn handle(&mut self, msg: GatewayRegisterStart, _ctx: &mut Context<Self>) -> Result<()> {
        for register_address in self.register_address.iter() {
            match RegisterClient::new(register_address.clone(), self.gm.clone()).await {
                Ok(register_client) => {
                    self.register_client = Some(register_client);
                    self.register_address_current = register_address.clone();
                    break;
                }
                Err(_) => {
                    continue;
                }
            }
        }
        Ok(())
    }
}

/// 注册中心关闭触发
#[async_trait::async_trait]
impl Handler<RegisterClose> for GatewayManagement {
    async fn handle(&mut self, msg: RegisterClose, _ctx: &mut Context<Self>) -> bool {
        let register = self.register_client.as_mut().unwrap();
        let mut status = false;
        let _ = register.gateway_tryconnection().await;
        status = register.register_status;
        return status;
    }
}

///注册中心来消息
///
#[async_trait::async_trait]
impl Handler<RegisterMessage> for GatewayManagement {
    async fn handle(&mut self, msg: RegisterMessage, _ctx: &mut Context<Self>) -> Result<()> {
        // 接收tcp
        let event = String::from_utf8(msg.0).unwrap_or_default();
        if let Ok(event) = serde_json::from_str::<RegisterEvent>(&event) {
            self.gateway_address_process(event.addresses).await;
        } else {
            // todo 解析失败的处理
            error!("RegisterMessage error {:?}  ", event);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<RegisterStatus> for GatewayManagement {
    async fn handle(&mut self, message: RegisterStatus, ctx: &mut Context<Self>) -> bool {
        true
    }
}

// 发送数到网关
#[async_trait::async_trait]
impl Handler<GatewayRequest> for GatewayManagement {
    async fn handle(&mut self, msg: GatewayRequest, ctx: &mut Context<Self>) -> Result<()> {
        if msg.address.is_none() {
            for (addr, gateway) in self.connect_gateway.iter_mut() {
                let data = msg.respond_to.clone();
                let _ = gateway.send(data).await?;
            }
        } else {
            for addr in msg.address.unwrap().iter() {
                let address = format!("{}:{}", long2ip(addr.0), addr.1);
                if let Some(gateway) = self.connect_gateway.get_mut(&address) {
                    let data = msg.respond_to.clone();

                    let _ = gateway.send(data).await?;
                };
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<GatewayToStatus> for GatewayManagement {
    async fn handle(&mut self, message: GatewayToStatus, ctx: &mut Context<Self>) -> i64 {
        if !message.opAdd {
            self.connect_gateway.remove(&message.address);
            self.close_gateway.insert(message.address, 1);
        }
        1
    }
}
