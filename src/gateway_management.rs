use std::collections::HashMap;
use xtra::Address;

use crate::dispatcher::DispatcherService;
use crate::gateway_client::GatewayClient;
use crate::register::RegisterClient;
use crate::types::*;

pub struct GatewayManagement {
    pub register_address: Vec<String>,

    pub register_address_current: String,

    // 已经连接的网关
    pub connect_gateway: HashMap<String, GatewayClient>,

    //所有的网关
    all_gateway: HashMap<String, i32>,

    //连接中的网关
    pub wait_gateway: HashMap<String, GatewayClient>,

    // 断开的网关
    pub close_gateway: HashMap<String, i32>,

    //发起请求到网关的序列号返回数据
    // gateway_seq: HashMap<i32, HashMap<String, Vec<u8>>>,
    pub dispatcher_service: Address<DispatcherService>,

    pub register_client: Option<RegisterClient>,

    pub gm: Address<GatewayManagement>,
}

impl GatewayManagement {
    pub async fn new(
        register_address: Vec<String>,
        gm: Address<GatewayManagement>,
        dispatcher_service: Address<DispatcherService>,
    ) -> Self {
        GatewayManagement {
            register_address,
            connect_gateway: HashMap::new(),
            all_gateway: HashMap::new(),
            wait_gateway: HashMap::new(),
            close_gateway: HashMap::new(),
            // gateway_seq: HashMap::new(),
            // gateway_client_req_mpsc_tx: mpsc_tx,
            dispatcher_service,
            register_client: None,
            register_address_current: "".to_string(),
            gm,
        }
    }

    /// 接收register返回的gateway的地址
    pub async fn gateway_address_process(&mut self, addresses: Vec<String>) {
        // 清空数据
        let old_all: Vec<String> = self.all_gateway.clone().into_keys().collect();
        self.all_gateway.clear();
        for addr in addresses.iter() {
            self.all_gateway.insert(addr.clone(), 1);
            self.check_gateway_connections(addr.clone()).await;
        }
        // 已经下线的
        let del_diff = old_all
            .iter()
            .filter(|&x| !addresses.contains(x))
            .collect::<Vec<_>>();

        // 新增上线的
        let add_diff = addresses
            .iter()
            .filter(|&x| !old_all.contains(x))
            .collect::<Vec<_>>();
    }

    /// 检查是否已经连接
    pub async fn check_gateway_connections(&mut self, address: String) {
        if self.all_gateway.contains_key(&address) == true
            && self.connect_gateway.contains_key(&address) != true
            && self.wait_gateway.contains_key(&address) != true
        {
            //实现gateway client
            let dispatcher_service = self.dispatcher_service.clone();
            match GatewayClient::new(dispatcher_service, address.clone()).await {
                Ok(gateway_client) => {
                    //网关连接成功
                    self.connect_gateway.insert(address.clone(), gateway_client);
                    let _ = self
                        .dispatcher_service
                        .do_send_async(GatewayResponse {
                            address: address.clone(),
                            method: GatewayMethod::Start,
                            respond_to: vec![],
                        })
                        .await;
                }
                Err(e) => {
                    println!("gateway client error {:?}", e);
                }
            }
        }
    }

    // 接收网关返回的数据
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    #[test]
    fn a() {
        println!("a");
    }
}
