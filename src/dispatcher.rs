use anyhow::Result;
use chrono::prelude::*;
use iptools::ipv4::{ip2long, long2ip};
use serde_json::json;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration, Instant};
use xtra::Address;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use crate::gateway_management::GatewayManagement;
use crate::types::{
    gateway_protocol::{Context, ExtData, PHPGateSelectWhere, PHPGatewayProtocol},
    ChannelGatewayData, GatewayRequest, GatewaySeq, LocalAddress, ProtocolData,
};

pub struct DispatcherService {
    pub req_queue: HashMap<String, VecDeque<GatewaySeq>>, // mpa<"127.0.0.1:2900", Vec<GatewaySeq>>
    pub gateway_managen: Address<GatewayManagement>,
    pub seq: AtomicU32,
    pub time_out: u64,
}

impl DispatcherService {
    pub fn new(gateway_managen: Address<GatewayManagement>) -> Self {
        DispatcherService {
            req_queue: HashMap::new(),
            gateway_managen,
            seq: AtomicU32::new(0),
            time_out: 2000,
        }
    }

    /// 设备是否在线
    pub async fn is_online(
        &mut self,
        client_id: String,
        tx: mpsc::Sender<ChannelGatewayData>,
    ) -> Result<()> {
        let seq = self.seq();
        let address_data = Context::client_id_to_address(client_id).unwrap_or(LocalAddress::new());
        let address = format!(
            "{}:{}",
            long2ip(address_data.local_ip),
            address_data.local_port
        );

        let mut gatewayprotocol = PHPGatewayProtocol::new();
        gatewayprotocol.cmd = PHPGatewayProtocol::CMD_IS_ONLINE;
        gatewayprotocol.set_connection_id(address_data.connection_id);
        let buffer = gatewayprotocol.encode();

        let gateway_managen = self.gateway_managen.clone();

        let now = Local::now();
        let gateway_seq = GatewaySeq {
            seq,
            time: now.timestamp_millis(),
            tx: tx,
        };

        if let Some(queue) = self.req_queue.get_mut(&address) {
            queue.push_front(gateway_seq.clone());
        }

        // 请求
        let req = GatewayRequest {
            seq,
            respond_to: buffer,
            address: Some(vec![(address_data.local_ip, address_data.local_port)]),
            // address: None,
            tx_response: None,
        };
        let _ = gateway_managen.send(req).await.unwrap();
        return Ok(());
    }

    /// 向所有client绑定的uid发送消息
    /// vec<String> uid
    /// 发送数据 body
    pub async fn send_to_uid(&mut self, uid: Vec<String>, body: String) -> Result<()> {
        let seq = self.seq();
        let mut gatewayprotocol = PHPGatewayProtocol::new();
        gatewayprotocol.cmd = PHPGatewayProtocol::CMD_SEND_TO_UID;
        let body = ProtocolData::DataStr::<String>(body);
        gatewayprotocol.set_body(body);
        let ext_data = json!(uid);
        gatewayprotocol.set_ext_data(ext_data.to_string());
        let buffer = gatewayprotocol.encode();
        let gateway_managen = self.gateway_managen.clone();
        let req = GatewayRequest {
            seq,
            respond_to: buffer,
            address: None,
            tx_response: None,
        };
        let _ = gateway_managen.send(req).await?;
        Ok(())
    }

    /// 加入组
    /// * 资源id client_id
    /// * 组名称 group
    pub async fn join_group(&mut self, client_id: String, group: String) -> Result<()> {
        let seq = self.seq();
        let address_data = Context::client_id_to_address(client_id).unwrap_or(LocalAddress::new());
        let address = format!(
            "{}:{}",
            long2ip(address_data.local_ip),
            address_data.local_port
        );

        // 构建数据
        let body = ProtocolData::DataStr::<String>("".to_string());
        let mut gatewayprotocol = PHPGatewayProtocol::new();
        gatewayprotocol.cmd = PHPGatewayProtocol::CMD_JOIN_GROUP;
        gatewayprotocol.set_connection_id(address_data.connection_id);
        gatewayprotocol.set_ext_data(group);
        let buffer = gatewayprotocol.encode();

        let gateway_managen = self.gateway_managen.clone();
        let req = GatewayRequest {
            seq,
            respond_to: buffer,
            address: Some(vec![(address_data.local_ip, address_data.local_port)]),
            tx_response: None,
        };
        let _ = gateway_managen.send(req).await?;

        Ok(())
    }

    /// 向 group 发送
    /// * group             组（不允许是 0 '0' false null array()等为空的值）
    /// * message           消息
    /// * exclude_client_id 不给这些client_id发
    /// * raw               发送原始数据（即不调用gateway的协议的encode方法）
    pub async fn send_to_group(
        &mut self,
        group: Vec<String>,
        message: String,
        exclude_client_id: Option<Vec<String>>,
        raw: Option<bool>,
    ) -> Result<()> {
        if group.is_empty() {
            return Ok(());
        }
        let mut gate_protocol = PHPGatewayProtocol::new();
        gate_protocol.cmd = PHPGatewayProtocol::CMD_SEND_TO_GROUP;

        if let Some(x) = raw {
            gate_protocol.flag |= PHPGatewayProtocol::FLAG_NOT_CALL_ENCODE;
        }
        let body = ProtocolData::DataStr::<String>(message);
        gate_protocol.set_body(body);

        // 分组发送，没有排除的client_id，直接发送
        let default_ext_data_buffer = json!({
            "group": group,
            "exclude": null,
        });

        let seq = self.seq();
        let gateway_managen = self.gateway_managen.clone();
        if exclude_client_id.is_none() {
            gate_protocol.set_ext_data(default_ext_data_buffer.to_string());
            let buffer = gate_protocol.encode();
            let req = GatewayRequest {
                seq,
                respond_to: buffer,
                address: None,
                tx_response: None,
            };
            let _ = gateway_managen.send(req).await?;
        }

        Ok(())
    }

    fn seq(&mut self) -> u32 {
        return self.seq.fetch_add(1, Ordering::SeqCst);
    }

    /// 离开组
    /// 资源id client_id
    /// 组名称 group
    pub async fn leave_group(&mut self, client_id: String, group: String) -> Result<()> {
        let seq = self.seq();
        let address_data = Context::client_id_to_address(client_id).unwrap_or(LocalAddress::new());
        let address = format!(
            "{}:{}",
            long2ip(address_data.local_ip),
            address_data.local_port
        );

        let mut gate_protocol = PHPGatewayProtocol::new();
        gate_protocol.cmd = PHPGatewayProtocol::CMD_LEAVE_GROUP;
        gate_protocol.set_ext_data(group);
        let buffer = gate_protocol.encode();
        let gateway_managen = self.gateway_managen.clone();
        let req = GatewayRequest {
            seq,
            respond_to: buffer,
            address: Some(vec![(address_data.local_ip, address_data.local_port)]),
            tx_response: None,
        };
        let _ = gateway_managen.send(req).await?;
        Ok(())
    }

    pub async fn bind_uid(&mut self, client_id: String, uid: String) -> Result<()> {
        let seq = self.seq();
        let address_data = Context::client_id_to_address(client_id).unwrap_or(LocalAddress::new());
        let address = format!(
            "{}:{}",
            long2ip(address_data.local_ip),
            address_data.local_port
        );

        let mut gate_protocol = PHPGatewayProtocol::new();
        gate_protocol.cmd = PHPGatewayProtocol::CMD_BIND_UID;
        gate_protocol.set_ext_data(uid);
        gate_protocol.set_connection_id(address_data.connection_id);
        let buffer = gate_protocol.encode();
        let gateway_managen = self.gateway_managen.clone();
        let req = GatewayRequest {
            seq,
            respond_to: buffer,
            address: Some(vec![(address_data.local_ip, address_data.local_port)]),
            tx_response: None,
        };
        let _ = gateway_managen.send(req).await?;
        Ok(())
    }

    /// 获取uid 绑定的 client_id 列表
    /// uid: 查询的uid
    pub async fn is_uid_online(
        &mut self,
        uid: String,
        tx: mpsc::Sender<ChannelGatewayData>,
    ) -> Result<()> {
        let seq = self.seq();
        let mut gatewayprotocol = PHPGatewayProtocol::new();
        gatewayprotocol.cmd = PHPGatewayProtocol::CMD_GET_CLIENT_ID_BY_UID;
        gatewayprotocol.set_ext_data(uid);
        let buffer = gatewayprotocol.encode();

        let now = Local::now();
        let gateway_seq = GatewaySeq {
            seq,
            time: now.timestamp_millis(),
            tx,
        };

        for (addres, queue) in self.req_queue.iter_mut() {
            queue.push_front(gateway_seq.clone());
        }

        let gateway_managen = self.gateway_managen.clone();
        // 请求
        let req = GatewayRequest {
            seq,
            respond_to: buffer,
            // address: Some(vec![(address_data.local_ip, address_data.local_port)]),
            address: None,
            tx_response: None,
        };
        let _ = gateway_managen.send(req).await.unwrap();
        return Ok(());
    }

    /// 获取group在线uid列表
    /// group: 查询的group
    /// tx: 接收数据通道
    pub async fn get_uid_list_group(
        &mut self,
        group: Vec<String>,
        tx: mpsc::Sender<ChannelGatewayData>,
    ) -> Result<()> {
        let seq = self.seq();
        let mut fields = Vec::new();
        fields.push("uid".to_string());
        let mut wheres = HashMap::new();
        wheres.insert("groups".to_string(), group);
        let mut buffer_map = self.select(fields, wheres).await?;
        let gateway_managen = self.gateway_managen.clone();
        let now = Local::now();
        let gateway_seq = GatewaySeq {
            seq,
            time: now.timestamp_millis(),
            tx,
        };

        if !buffer_map.contains_key(&(1, 1)) {
            for ((ip, port), mut b) in buffer_map.into_iter() {
                let addr = format!("{}:{}", long2ip(ip), port);
                if let Some(queue) = self.req_queue.get_mut(&addr) {
                    queue.push_front(gateway_seq.clone());
                }

                let buffer = b.encode();
                // 请求
                let req = GatewayRequest {
                    seq,
                    respond_to: buffer,
                    // address: Some(vec![(address_data.local_ip, address_data.local_port)]),
                    address: None,
                    tx_response: None,
                };
                let _ = gateway_managen.send(req).await?;
            }
        } else {
            if let Some(buffer) = buffer_map.get_mut(&(1, 1)) {
                for (addres, queue) in self.req_queue.iter_mut() {
                    queue.push_front(gateway_seq.clone());
                }
                // 请求
                let req = GatewayRequest {
                    seq,
                    respond_to: buffer.encode(),
                    // address: Some(vec![(address_data.local_ip, address_data.local_port)]),
                    address: None,
                    tx_response: None,
                };
                let _ = gateway_managen.send(req).await?;
            }
        }

        Ok(())
    }

    /// 根据条件到gateway搜索数据
    /// fields: 需要查询的字段 [session, uid, groups]
    /// where: 查询条件 {client_id: [1,2,3], uid: [1,2,3], groups: [1,2,3]}
    /// 返回hashmap<addresss, response>
    pub async fn select(
        &mut self,
        mut fields: Vec<String>,
        mut wheres: HashMap<String, Vec<String>>,
    ) -> Result<HashMap<(u32, u16), PHPGatewayProtocol>> {
        let mut gate_protocol = PHPGatewayProtocol::new();
        gate_protocol.cmd = PHPGatewayProtocol::CMD_SELECT;

        if fields.is_empty() {
            fields.push("session".to_owned());
            fields.push("uid".to_owned());
            fields.push("groups".to_owned());
        }
        let mut wheres_clone = wheres.clone();
        let groups = wheres_clone.remove("groups").unwrap_or_else(|| {
            let g: Vec<String> = Vec::new();
            return g;
        });
        let uid = wheres_clone.remove("uid").unwrap_or_else(|| {
            let g: Vec<String> = Vec::new();
            return g;
        });

        let mut ext_data = ExtData {
            fields,
            r#where: PHPGateSelectWhere::new(uid.clone(), HashMap::new(), groups),
        };

        let mut gateway_data_list: HashMap<(u32, u16), PHPGatewayProtocol> = HashMap::new();
        if wheres.contains_key("client_id") {
            let client_id_list = wheres["client_id"].clone();
            //临时存储gw下要过来的cid
            let mut ips: HashMap<(u32, u16), HashMap<String, String>> = HashMap::new();

            for id in client_id_list {
                let address_data =
                    Context::client_id_to_address(id.to_string()).unwrap_or(LocalAddress::new());
                if address_data.local_ip == 0 {
                    continue;
                }
                let address = (address_data.local_ip, address_data.local_port);

                let connection_id = address_data.connection_id.to_string();
                ips.entry(address.clone()).or_insert(HashMap::new());

                if let Some(connection_ids) = ips.get_mut(&address) {
                    connection_ids
                        .entry(connection_id.clone())
                        .or_insert(connection_id.clone());
                }

                gateway_data_list
                    .entry(address)
                    .or_insert(gate_protocol.clone());
            }

            // 发送到指定gateway机器上的数据
            for (key, gateway) in gateway_data_list.iter_mut() {
                let new_connection_ids = ips[key].clone();
                let mut client_ext_data = ext_data.clone();
                client_ext_data.r#where.connection_id = new_connection_ids;
                let ext = serde_json::to_string(&client_ext_data).unwrap_or(String::new());
                gateway.set_ext_data(ext);
            }

            // 有其它条件，则还是需要向所有gateway发送
            if wheres.len() != 1 {
                let ext = serde_json::to_string(&ext_data).unwrap_or(String::new());
                gate_protocol.set_ext_data(ext);
                gateway_data_list.clear();
                gateway_data_list.insert((1, 1), gate_protocol);
            }

            // 返回条件过滤后的查询gateway地址
        } else {
            // 查询所有网关
            let ext = serde_json::to_string(&ext_data).unwrap_or(String::new());
            gate_protocol.set_ext_data(ext);
            gateway_data_list.insert((1, 1), gate_protocol);
        }

        return Ok(gateway_data_list);
    }

    /// 获取uid 绑定的 client_id 列表
    pub async fn get_client_id_by_uid(
        &mut self,
        uid: String,
        tx: mpsc::Sender<ChannelGatewayData>,
    ) -> Result<()> {
        let seq = self.seq();
        let mut gatewayprotocol = PHPGatewayProtocol::new();
        gatewayprotocol.cmd = PHPGatewayProtocol::CMD_GET_CLIENT_ID_BY_UID;
        gatewayprotocol.set_ext_data(uid);
        let buffer = gatewayprotocol.encode();
        let now = Local::now();
        let gateway_seq = GatewaySeq {
            seq,
            time: now.timestamp_millis(),
            tx,
        };

        for (addres, queue) in self.req_queue.iter_mut() {
            queue.push_front(gateway_seq.clone());
        }

        let gateway_managen = self.gateway_managen.clone();
        // 请求
        let req = GatewayRequest {
            seq,
            respond_to: buffer,
            address: None,
            tx_response: None,
        };
        let _ = gateway_managen.do_send_async(req).await?;

        Ok(())
    }
}
