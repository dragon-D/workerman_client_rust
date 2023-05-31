pub mod gateway_protocol;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use xtra::prelude::*;

pub type ChannelGatewayData = (String, Vec<u8>);

pub enum CValue {
    one(oneshot::Sender<ChannelGatewayData>),
    mpsc(mpsc::Sender<ChannelGatewayData>),
}

/// 解析出id中的数据结构
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct LocalAddress {
    pub local_ip: u32,
    pub local_port: u16,
    pub connection_id: u32,
}

impl LocalAddress {
    pub fn new() -> Self {
        Self {
            local_ip: 0,
            local_port: 0,
            connection_id: 0,
        }
    }
}

#[derive(Debug)]
pub enum ActionMessage {
    IsOnline {
        request: String,
        tx: mpsc::Sender<ChannelGatewayData>,
    },
    SendToGroup {
        group: Vec<String>,
        message: String,
        exclude_client_id: Option<Vec<String>>,
        raw: Option<bool>,
    },
    SendToUid {
        uid: Vec<String>,
        body: String,
    },
    JoinGroup {
        client_id: String,
        group: String,
    },
    LeaveGroup {
        client_id: String,
        group: String,
    },
    UnBindUid {
        client_id: String,
        uid: String,
    },
    BindUid {
        client_id: String,
        uid: String,
    },
    GetUidListGroup {
        group: Vec<String>,
        tx: mpsc::Sender<ChannelGatewayData>,
    },
    GetClientIdByUid {
        uid: String,
        tx: mpsc::Sender<ChannelGatewayData>,
    },
    IsUidOnline {
        uid: String,
        tx: mpsc::Sender<ChannelGatewayData>,
    },
}

impl Message for ActionMessage {
    type Result = Result<()>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegisterEvent {
    pub event: String,
    pub addresses: Vec<String>,
}

/// 网关返回的数据
#[derive(Debug)]
pub struct GatewayResponse {
    pub respond_to: Vec<u8>,
    pub method: GatewayMethod,
    pub address: String,
}

#[derive(Debug)]
pub enum GatewayMethod {
    Stop,
    Send,
    Close,
    Start,
}

impl Message for GatewayResponse {
    type Result = Result<bool>;
}

// dispatch记录请求的序列顺序
#[derive(Debug, Clone)]
pub struct GatewaySeq {
    pub seq: u32,
    pub time: i64,
    pub tx: mpsc::Sender<ChannelGatewayData>,
}

/// 发送的数据
#[derive(Debug)]
pub struct GatewayRequest {
    pub seq: u32,
    pub respond_to: Vec<u8>,
    pub address: Option<Vec<(u32, u16)>>,
    pub tx_response: Option<mpsc::Sender<ChannelGatewayData>>,
}

impl Message for GatewayRequest {
    type Result = Result<()>;
}

/// 网关的发送body类型
#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum ProtocolData<T> {
    DataStr(String),

    DataObject(T),

    DataInt(u32),
}

/// 注册中心开始
#[derive(Debug)]
pub struct GatewayRegisterStart();

impl Message for GatewayRegisterStart {
    type Result = Result<(bool)>;
}

/// 调度器初始化开始
pub struct DispatcherStart();

impl Message for DispatcherStart {
    type Result = Result<()>;
}

/// 网关连接成功
pub struct GatewayConnectionSucess {
    pub data: Vec<u8>,
    pub address: String,
}

impl Message for GatewayConnectionSucess {
    type Result = Result<()>;
}

/// 注册中心连接断开
pub struct RegisterClose(pub String);

impl Message for RegisterClose {
    type Result = bool;
}

/// 注册中心消息
pub struct RegisterMessage(pub Vec<u8>);

impl Message for RegisterMessage {
    type Result = Result<()>;
}

/// 注册中心状态
pub struct RegisterStatus();

impl Message for RegisterStatus {
    type Result = bool;
}

/// 在线网关address
pub struct GatewayToStatus {
    pub address: String,
    pub opAdd: bool,
}

impl Message for GatewayToStatus {
    type Result = i64;
}
