pub mod dispatcher;
pub mod gateway_client;
pub mod gateway_management;
pub mod register;
pub mod service;
pub mod types;

use crate::dispatcher::DispatcherService;
use crate::gateway_management::GatewayManagement;
use crate::register::RegisterClient;
use crate::types::{DispatcherStart, *};
use std::collections::HashMap;

use anyhow::Result;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout, Duration};
use xtra::{Address, Context};

use crate::types::gateway_protocol::{
    decode_body_int, decode_body_map, decode_body_vec, PHPGatewayResponse,
};
use std::env::set_var;

#[macro_use]
extern crate log;
extern crate env_logger;

pub async fn run_dispatcher(address: Vec<String>) -> Address<DispatcherService> {
    env_logger::init();
    let (gateway_managen_service, gateway_managen_services_ctx) = Context::new(None);
    let (dispatcher_service, dispatcher_services_ctx) = Context::new(None);

    let gm_actor = GatewayManagement::new(
        address,
        gateway_managen_service.clone(),
        dispatcher_service.clone(),
    )
    .await;
    tokio::spawn(async move {
        gateway_managen_services_ctx.run(gm_actor).await;
    });

    // 注册中心加载运行
    let _ = gateway_managen_service
        .do_send_async(GatewayRegisterStart())
        .await;

    // 调度任务
    let d_actor = DispatcherService::new(gateway_managen_service.clone());
    tokio::spawn(async move {
        dispatcher_services_ctx.run(d_actor).await;
    });

    sleep(Duration::from_secs(1)).await;
    return dispatcher_service;
}

/// 查询设备是否在线
pub async fn is_online(dispatch: Address<DispatcherService>, client_id: &str) -> u32 {
    let (tx, mut rx) = mpsc::channel(100);
    let m = ActionMessage::IsOnline {
        request: client_id.to_string(),
        tx: tx,
    };
    let _ = dispatch.send(m).await;
    let res = timeout(Duration::from_secs(5), async {
        let mut onlione = 0;
        while let Some(msg) = rx.recv().await {
            onlione = decode_body_int(msg.1.as_slice());
            break;
        }
        return onlione;
    })
    .await
    .unwrap_or(0);
    return res;
}

/// 获取分组下的在线用户
pub async fn get_uid_list_group(
    dispatch: Address<DispatcherService>,
    group: Vec<String>,
) -> HashMap<String, HashMap<u32, PHPGatewayResponse>> {
    let (tx, mut rx) = mpsc::channel(100);
    let m = ActionMessage::GetUidListGroup {
        group: group,
        tx: tx,
    };
    let mut res_map = HashMap::new();
    let _ = dispatch.send(m).await;
    match timeout(Duration::from_secs(5), async {
        while let Some(msg) = rx.recv().await {
            res_map.insert(msg.0, msg.1);
        }
    })
    .await
    {
        Ok(_) => (),
        Err(_) => {
            error!("get_uid_list_group timeout");
        }
    }
    let mut gateway_res = HashMap::new();
    for (k, v) in res_map.iter() {
        let d = decode_body_map(v);
        gateway_res.insert(k.to_string(), d);
    }
    return gateway_res;
}

/// 获取用户在线设备
pub async fn get_client_id_by_uid(
    dispatch: Address<DispatcherService>,
    user_id: &str,
) -> HashMap<String, Vec<u32>> {
    let (tx, mut rx) = mpsc::channel(100);
    let m = ActionMessage::GetClientIdByUid {
        uid: user_id.to_owned(),
        tx: tx,
    };
    let mut res_map = HashMap::new();
    let _ = dispatch.send(m).await;
    match timeout(Duration::from_secs(5), async {
        while let Some(msg) = rx.recv().await {
            res_map.insert(msg.0, msg.1);
        }
    })
    .await
    {
        Ok(_) => (),
        Err(_) => {
            error!("get_uid_list_group timeout");
        }
    }
    let mut gateway_res = HashMap::new();
    for (k, v) in res_map.iter() {
        let d = decode_body_vec(v);
        gateway_res.insert(k.to_string(), d);
    }
    return gateway_res;
}

/// 分组下广播所有在线用户
pub async fn send_group(
    dispatch: Address<DispatcherService>,
    mesaage: &str,
    group: &str,
    exclude_client_id: Option<Vec<String>>,
    raw: Option<bool>,
) -> Result<()> {
    let msg = ActionMessage::SendToGroup {
        message: mesaage.to_string(),
        group: vec![group.to_string()],
        exclude_client_id: exclude_client_id,
        raw: raw,
    };
    let res = dispatch.do_send_async(msg).await?;
    Ok(())
}

/// 加入分组
pub async fn cid_join_group(
    dispatch: Address<DispatcherService>,
    client_id: &str,
    group: &str,
) -> Result<()> {
    let msg = ActionMessage::JoinGroup {
        client_id: client_id.to_string(),
        group: group.to_string(),
    };
    let res = dispatch.do_send_async(msg).await?;
    Ok(())
}

/// 给bind uid所有在线设备广播
pub async fn send_uid(
    dispatch: Address<DispatcherService>,
    uid: &str,
    message: &str,
) -> Result<()> {
    let msg = ActionMessage::SendToUid {
        uid: vec![uid.to_string()],
        body: message.to_string(),
    };
    let res = dispatch.do_send_async(msg).await?;
    Ok(())
}

/// 退出组
pub async fn leave_group(
    dispatch: Address<DispatcherService>,
    cid: &str,
    group: &str,
) -> Result<()> {
    let msg = ActionMessage::LeaveGroup {
        client_id: cid.to_string(),
        group: group.to_string(),
    };
    let res = dispatch.do_send_async(msg).await?;
    Ok(())
}

/// 给bind uid所有在线设备广播
pub async fn bind_uid(dispatch: Address<DispatcherService>, cid: &str, uid: &str) -> Result<()> {
    let msg = ActionMessage::BindUid {
        client_id: cid.to_string(),
        uid: uid.to_string(),
    };
    let res = dispatch.do_send_async(msg).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::gateway_protocol::ResponseType::vec;
    use tokio::time::{sleep, Duration};

    #[actix_rt::test]
    async fn send_to_uid() {
        let dispatcher_service = run_dispatcher(vec!["127.0.0.1:1238".to_string()]).await;
        let _ = send_uid(
            dispatcher_service,
            "86490735033065472",
            "这是rust sdk发送的",
        )
        .await;
        sleep(Duration::from_secs(1)).await;
        println!("done");
    }

    /// 加入组同时往组推送内如
    #[actix_rt::test]
    async fn test_join_gorup() {
        let dispatcher_service = run_dispatcher(vec!["127.0.0.1:1238".to_string()]).await;
        let _ = cid_join_group(dispatcher_service.clone(), "7f0000010b540000000c", "group").await;
        sleep(Duration::from_secs(1)).await;
        let _ = send_group(
            dispatcher_service.clone(),
            "呼叫group 1",
            "group",
            None,
            None,
        )
        .await;
        sleep(Duration::from_secs(1)).await;
        println!("done join_gorup");
    }

    #[actix_rt::test]
    async fn test_send_group() {
        let dispatcher_service = run_dispatcher(vec!["127.0.0.1:1238".to_string()]).await;
        let _ = send_group(
            dispatcher_service.clone(),
            "rust sdk 呼叫group 1",
            "86490735033065472",
            None,
            None,
        )
        .await;
        sleep(Duration::from_secs(1)).await;
        println!("done");
    }

    #[actix_rt::test]
    async fn test_leave_group() {
        let dispatcher_service = run_dispatcher(vec!["127.0.0.1:1238".to_string()]).await;
        let _ = cid_join_group(dispatcher_service.clone(), "7f0000010b540000000c", "group").await;
        sleep(Duration::from_secs(1)).await;

        // 发消息
        let _ = send_group(
            dispatcher_service.clone(),
            "rust sdk 呼叫group 1",
            "group",
            None,
            None,
        )
        .await;

        sleep(Duration::from_secs(1)).await;

        // 退出组
        let _ = leave_group(dispatcher_service.clone(), "7f0000010b540000000c", "group").await;

        sleep(Duration::from_secs(1)).await;

        // 发消息不应该受到
        let _ = send_group(
            dispatcher_service.clone(),
            "rust sdk 呼叫group 2",
            "gruop",
            None,
            None,
        )
        .await;

        sleep(Duration::from_secs(1)).await;
        println!("done");
    }

    #[actix_rt::test]
    async fn test_bind_uid() {
        let dispatcher_service = run_dispatcher(vec!["127.0.0.1:1238".to_string()]).await;
        let _ = bind_uid(dispatcher_service.clone(), "7f0000010b5400000002", "uid1").await;

        sleep(Duration::from_secs(1)).await;

        // 发消息
        let _ = send_uid(dispatcher_service, "uid1", "这是rust sdk发送给uid1").await;

        sleep(Duration::from_secs(1)).await;
        println!("done");
    }

    #[actix_rt::test]
    async fn cid_online() {
        let dispatcher_service = run_dispatcher(vec!["127.0.0.1:1238".to_string()]).await;
        let online = is_online(dispatcher_service, "7f0000010b5400000008").await;
        println!("done cid_online online={}", online);
    }

    #[actix_rt::test]
    async fn get_by_uid() {
        let dispatcher_service = run_dispatcher(vec!["127.0.0.1:1238".to_string()]).await;
        let client_id = get_client_id_by_uid(dispatcher_service, "86490735033065472").await;
        println!("done get_by_uid uid={:?}", client_id);
    }

    #[actix_rt::test]
    async fn get_group() {
        let dispatcher_service = run_dispatcher(vec!["127.0.0.1:1238".to_string()]).await;
        let groups =
            get_uid_list_group(dispatcher_service, vec!["86490735033065472".to_string()]).await;
        println!("done get_group group={:?}", groups);
    }
}
