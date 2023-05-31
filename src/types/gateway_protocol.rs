use anyhow::{anyhow, Result};
use bincode::Options;
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use iptools::ipv4::{ip2long, long2ip};
use serde::{Deserialize, Serialize};
use serde_php::{from_bytes, to_vec};
use std::collections::HashMap;

use super::{LocalAddress, ProtocolData};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PHPGatewayProtocol {
    pub pack_len: u32,
    pub cmd: u8,
    pub local_ip: u32,
    pub local_port: u16,
    pub client_ip: u32,
    pub client_port: u16,
    pub connection_id: u32,
    pub flag: u8,
    pub gateway_port: u16,
    pub ext_len: Option<u32>,
    pub ext_data: Option<String>,
    pub body: Option<String>,
}

pub struct Context {
    /// 内部通信ip
    pub local_ip: String,

    /// 内部通信断开
    pub local_port: u16,

    /// 客户ip
    pub client_ip: String,

    /// 客户端口
    pub client_port: i16,

    /// client_id
    pub client_id: String,

    /// 连接connection->id
    pub connection_id: u32,

    /// 旧的session
    pub old_session: String,
}

impl Context {
    /// client_id 到通讯地址的转换
    pub fn client_id_to_address(client_id: String) -> Result<LocalAddress> {
        if client_id.len() != 20 {
            return Err(anyhow!("client_id {} is invalid", &client_id));
        }

        let hex_bin = hex::decode(&client_id)?;
        if hex_bin.len() != 10 {
            return Err(anyhow!("client_id {} is invalid", &client_id));
        }

        let options = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding()
            .with_big_endian()
            .allow_trailing_bytes();

        let protocol: LocalAddress = options.deserialize(&hex_bin[..])?;
        Ok(protocol)
    }

    /// client_id解析出connection_id
    pub fn get_connection_id(client_id: String) -> String {
        let address_data =
            Self::client_id_to_address(client_id.to_string()).unwrap_or(LocalAddress::new());
        if address_data.local_ip == 0 {
            return "".to_string();
        }

        let address = format!(
            "{}:{}",
            long2ip(address_data.local_ip),
            address_data.local_port
        );

        let connection_id = address_data.connection_id.to_string();
        connection_id
    }

    /// 通讯地址到 client_id 的转换
    /// ip local_ip
    /// port local_port
    /// id connection_id
    pub fn address_to_clientId(ip: &str, port: u16, id: u32) -> String {
        let ip = ip2long(ip).unwrap_or(0);
        if ip == 0 || id == 0 {
            return "".to_string();
        }
        let mut buffer = BytesMut::with_capacity(10);
        buffer.put_u32(ip);
        buffer.put_u16(port);
        buffer.put_u32(id);
        let bin2hex = hex::encode(buffer.to_vec());
        bin2hex
    }
}

impl PHPGatewayProtocol {
    pub const HEAD_LEN: u8 = 28;

    /// 发给worker，gateway有一个新的连接
    pub const CMD_ON_CONNECT: u8 = 1;

    /// 发给worker的，客户端有消息
    pub const CMD_ON_MESSAGE: u8 = 3;

    /// 发给worker上的关闭链接事件
    pub const CMD_ON_CLOSE: u8 = 4;

    /// 发给gateway的向单个用户发送数据
    pub const CMD_SEND_TO_ONE: u8 = 5;

    /// 发给gateway的向所有用户发送数据
    pub const CMD_SEND_TO_ALL: u8 = 6;

    /// 发给gateway的踢出用户
    /// 1、如果有待发消息，将在发送完后立即销毁用户连接
    /// 2、如果无待发消息，将立即销毁用户连接
    pub const CMD_KICK: u8 = 7;

    /// 发给gateway的立即销毁用户连接
    pub const CMD_DESTROY: u8 = 8;

    /// 发给gateway，通知用户session更新
    pub const CMD_UPDATE_SESSION: u8 = 9;

    /// 获取在线状态
    pub const CMD_GET_ALL_CLIENT_SESSIONS: u8 = 10;

    /// 判断是否在线
    pub const CMD_IS_ONLINE: u8 = 11;

    /// client_id绑定到uid
    pub const CMD_BIND_UID: u8 = 12;

    /// 解绑
    pub const CMD_UNBIND_UID: u8 = 13;

    /// 向uid发送数据
    pub const CMD_SEND_TO_UID: u8 = 14;

    /// 根据uid获取绑定的clientid
    pub const CMD_GET_CLIENT_ID_BY_UID: u8 = 15;

    /// 加入组
    pub const CMD_JOIN_GROUP: u8 = 20;

    /// 离开组
    pub const CMD_LEAVE_GROUP: u8 = 21;

    /// 向组成员发消息
    pub const CMD_SEND_TO_GROUP: u8 = 22;

    /// 获取组成员
    pub const CMD_GET_CLIENT_SESSIONS_BY_GROUP: u8 = 23;

    /// 获取组在线连接数
    pub const CMD_GET_CLIENT_COUNT_BY_GROUP: u8 = 24;

    /// 按照条件查找
    pub const CMD_SELECT: u8 = 25;

    /// 获取在线的群组ID
    pub const CMD_GET_GROUP_ID_LIST: u8 = 26;

    /// 取消分组
    pub const CMD_UNGROUP: u8 = 27;

    /// worker连接gateway事件
    pub const CMD_WORKER_CONNECT: u8 = 200;

    /// 心跳
    pub const CMD_PING: u8 = 201;

    /// GatewayClient连接gateway事件
    pub const CMD_GATEWAY_CLIENT_CONNECT: u8 = 202;

    /// 根据client_id获取session
    pub const CMD_GET_SESSION_BY_CLIENT_ID: u8 = 203;

    /// 发给gateway，覆盖session
    pub const CMD_SET_SESSION: u8 = 204;

    /// 当websocket握手时触发，只有websocket协议支持此命令字
    pub const CMD_ON_WEBSOCKET_CONNECT: u8 = 205;

    /// 包体是标量
    pub const FLAG_BODY_IS_SCALAR: u8 = 0x01;

    /// 通知gateway在send时不调用协议encode方法，在广播组播时提升性能
    pub const FLAG_NOT_CALL_ENCODE: u8 = 0x02;

    pub fn set_connection_id(&mut self, c_id: u32) {
        self.connection_id = c_id;
    }

    pub fn set_ext_data(&mut self, ext: String) {
        self.ext_data = Some(ext);
    }

    pub fn set_body<T: Serialize + Clone>(&mut self, body: ProtocolData<T>) {
        // let body = serde_json::to_string(&body).unwrap_or(String::new());
        // let c = ProtocolData::DataObject::<T>(body);
        let mut body_str = String::new();
        let mut flag = 0u8;
        match body {
            ProtocolData::DataInt(body) => {
                body_str = body.to_string();
                flag = 1;
            }
            ProtocolData::DataStr(body) => {
                body_str = body;
                flag = 1;
            }
            ProtocolData::DataObject(body) => {
                let b = to_vec(&body).unwrap_or(Vec::new());
                body_str = String::from_utf8(b).unwrap_or(String::new());
                flag = 0;
            }
        }
        self.body = Some(body_str);
        self.flag = flag | self.flag;
        // self
    }

    pub fn new() -> Self {
        Self {
            pack_len: 0,
            cmd: 0,
            local_ip: 0,
            local_port: 0,
            client_ip: 0,
            client_port: 0,
            connection_id: 0,
            flag: 0,
            gateway_port: 0,
            ext_len: Some(0),
            ext_data: Some("".to_owned()),
            body: Some("".to_owned()),
        }
    }

    pub fn encode(&mut self) -> Vec<u8> {
        // let b = String::from_utf8_lossy(&body);
        let mut ext_data = String::new();
        let ext_len = if let Some(x) = &self.ext_data {
            ext_data = x.clone();
            x.len() as u32
        } else {
            0u32
        };
        let body = if let Some(x) = &self.body {
            x.as_bytes()
        } else {
            &[]
        };
        let package_len: usize = Self::HEAD_LEN as usize + ext_len as usize + body.len();
        let mut buf = BytesMut::with_capacity(package_len);

        buf.put_u32(package_len as u32);
        buf.put_u8(self.cmd);
        buf.put_u32(self.local_ip);
        buf.put_u16(self.local_port);
        buf.put_u32(self.client_ip);
        buf.put_u16(self.client_port);
        buf.put_u32(self.connection_id);
        buf.put_u8(self.flag);
        buf.put_u16(self.gateway_port);
        buf.put_u32(ext_len);
        buf.put(ext_data.as_bytes());
        buf.put(body);
        return buf.to_vec();
    }

    pub fn encodeBytesMut(&mut self) -> BytesMut {
        // let b = String::from_utf8_lossy(&body);
        let mut ext_data = String::new();
        let ext_len = if let Some(x) = &self.ext_data {
            ext_data = x.clone();
            x.len() as u32
        } else {
            0u32
        };
        let body = if let Some(x) = &self.body {
            x.as_bytes()
        } else {
            &[]
        };
        let package_len: usize = Self::HEAD_LEN as usize + ext_len as usize + body.len();
        let mut buf = BytesMut::with_capacity(package_len);

        buf.put_u32(package_len as u32);
        buf.put_u8(self.cmd);
        buf.put_u32(self.local_ip);
        buf.put_u16(self.local_port);
        buf.put_u32(self.client_ip);
        buf.put_u16(self.client_port);
        buf.put_u32(self.connection_id);
        buf.put_u8(self.flag);
        buf.put_u16(self.gateway_port);
        buf.put_u32(ext_len);
        buf.put(ext_data.as_bytes());
        buf.put(body);
        return buf;
    }

    pub fn decode(buff: Vec<u8>) -> PHPGatewayProtocol {
        // 解析出结构体
        let options = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding()
            .with_big_endian()
            .allow_trailing_bytes();
        let mut protocol: PHPGatewayProtocol = options
            .deserialize(&buff[..Self::HEAD_LEN as usize])
            .unwrap();
        let ext_len = protocol.big_u32(&buff[24..Self::HEAD_LEN as usize]);

        let mut ext_data = String::new();
        let mut body = String::new();
        if ext_len > 0 {
            let ext_data_buff = &buff[Self::HEAD_LEN as usize..ext_len as usize];
            ext_data = String::from_utf8_lossy(&ext_data_buff).to_string();
            if protocol.flag & Self::FLAG_BODY_IS_SCALAR == 1 {
                let body_buff = &buff[Self::HEAD_LEN as usize + ext_len as usize..];
                let body_str = String::from_utf8_lossy(body_buff).to_string();
                body = body_str;
            } else {
                //解开php的unserialize
                let body_buff = &buff[Self::HEAD_LEN as usize + ext_len as usize..];
                // let body_str = from_bytes(&body_buff).unwrap();
                // body = String::from_utf8_lossy(body_buff).to_string();
            }
        } else {
            if protocol.flag & Self::FLAG_BODY_IS_SCALAR == 1 {
                let body_buff = &buff[Self::HEAD_LEN as usize..];
                let body_str = String::from_utf8_lossy(body_buff).to_string();
                body = body_str;
            } else {
                //解开php的unserialize
                let body_buff = &buff[Self::HEAD_LEN as usize..];
                // let body_str = String::from_utf8_lossy(body_buff).to_string();
                // body = body_str;
            }
        }
        protocol.ext_len = Some(ext_len);
        protocol.ext_data = Some(ext_data);
        protocol.body = Some(body);

        protocol
    }

    pub fn big_u32(&self, buff: &[u8]) -> u32 {
        BigEndian::read_u32(buff)
    }

    pub fn big_u16(&self, buff: &[u8]) -> u16 {
        BigEndian::read_u16(buff)
    }

    pub fn big_u8(&self, buff: &[u8]) -> u8 {
        buff[0]
    }
}

/// gateway服务的中的处理字段
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PHPGateSelectWhere {
    pub uid: Vec<String>,
    pub connection_id: HashMap<String, String>,
    pub groups: Vec<String>,
}

impl PHPGateSelectWhere {
    pub fn new(
        uid: Vec<String>,
        connection_id: HashMap<String, String>,
        groups: Vec<String>,
    ) -> Self {
        Self {
            uid,
            connection_id,
            groups,
        }
    }

    pub fn count_where(&self) -> i32 {
        let total = self.uid.len() + self.connection_id.len() + self.groups.len();
        total as i32
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct ExtData {
    pub fields: Vec<String>,
    // r#where: Map<String, JValue>,
    pub r#where: PHPGateSelectWhere,
}

/// gateway select返回结果集
#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct PHPGatewayResponse {
    pub session: Option<String>,
    pub uid: Option<String>,
    // pub groups: Option<HashMap<u64, String>>,
    pub groups: Option<Vec<String>>,
}

pub enum ResponseType {
    map(HashMap<u32, PHPGatewayResponse>),
    vec(Vec<u32>),
    none,
}

pub fn decode_body_map(body: &[u8]) -> HashMap<u32, PHPGatewayResponse> {
    debug!("data_buffer = {:?}", body);
    let len = body.len();
    if len < 4 {
        return HashMap::new();
    }
    let data_buffer = &body[4..len];
    // let data_buffer = br#"a:1:{i:3;a:3:{s:7:"session";s:213:"a:7:{s:7:"user_id";s:17:"99050199523139584";s:9:"device_id";s:2:"12";s:7:"version";s:5:"1.3.0";s:17:"client_properties";s:0:"";s:9:"client_ip";s:7:"unknown";s:11:"online_time";i:1642341187;s:13:"guild_pending";N;}";s:3:"uid";s:3:"123";s:6:"groups";a:2:{i:0;s:1:"1";i:1;s:1:"2";}}}"#;
    match from_bytes::<HashMap<u32, PHPGatewayResponse>>(data_buffer) {
        Ok(php_arr) => {
            return php_arr;
        }
        Err(e) => {
            error!("php unserialize error data= {:?}", body);
            return HashMap::new();
        }
    }
}

pub fn decode_body_vec(body: &[u8]) -> Vec<u32> {
    debug!("data_buffer = {:?}", body);
    let len = body.len();
    if len < 4 {
        return vec![];
    }
    let data_buffer = &body[4..len];
    // let data_buffer = br#"a:1:{i:3;a:3:{s:7:"session";s:213:"a:7:{s:7:"user_id";s:17:"99050199523139584";s:9:"device_id";s:2:"12";s:7:"version";s:5:"1.3.0";s:17:"client_properties";s:0:"";s:9:"client_ip";s:7:"unknown";s:11:"online_time";i:1642341187;s:13:"guild_pending";N;}";s:3:"uid";s:3:"123";s:6:"groups";a:2:{i:0;s:1:"1";i:1;s:1:"2";}}}"#;
    match from_bytes::<Vec<u32>>(data_buffer) {
        Ok(r) => return r,
        Err(err) => {
            error!("php unserialize error data= {:?}", body);
            return vec![];
        }
    }
}

pub fn decode_body_int(body: &[u8]) -> u32 {
    // let string = body.as_bytes();
    debug!("data_buffer = {:?}", body);
    let len = body.len();
    if len < 4 {
        return 0;
    }
    let data_buffer = &body[4..len];
    // let data_buffer = br#"a:1:{i:3;a:3:{s:7:"session";s:213:"a:7:{s:7:"user_id";s:17:"99050199523139584";s:9:"device_id";s:2:"12";s:7:"version";s:5:"1.3.0";s:17:"client_properties";s:0:"";s:9:"client_ip";s:7:"unknown";s:11:"online_time";i:1642341187;s:13:"guild_pending";N;}";s:3:"uid";s:3:"123";s:6:"groups";a:2:{i:0;s:1:"1";i:1;s:1:"2";}}}"#;
    match from_bytes::<u32>(data_buffer) {
        Ok(num) => return num,
        Err(err) => {
            error!("php unserialize error data= {:?}", body);
            return 0;
        }
    }
}

#[test]
pub fn decode_php() {
    let data_buffer = br#"a:1:{i:3;a:3:{s:7:"session";s:213:"a:7:{s:7:"user_id";s:17:"99050199523139584";s:9:"device_id";s:2:"12";s:7:"version";s:5:"1.3.0";s:17:"client_properties";s:0:"";s:9:"client_ip";s:7:"unknown";s:11:"online_time";i:1642341187;s:13:"guild_pending";N;}";s:3:"uid";s:3:"123";s:6:"groups";a:2:{i:0;s:1:"1";i:1;s:1:"2";}}}"#;
    let php_arr = from_bytes::<HashMap<u32, PHPGatewayResponse>>(data_buffer).unwrap();
    println!("php {:?}", php_arr);
    assert_eq!(1,1)
}
