use anyhow::Result;
use bytes::{BytesMut};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};
use xtra::{Actor, Address};

use crate::dispatcher::DispatcherService;
use crate::types::{gateway_protocol::PHPGatewayProtocol, *};

type LineFramedStream = SplitStream<Framed<TcpStream, RstRespCodec>>;
type LineFramedSink = SplitSink<Framed<TcpStream, RstRespCodec>, RstResp>;

pub struct GatewayClient {
    // 当前状态
    status: bool,

    address: String,

    // rx: ReadHalf<TcpStream>,
    // tx: WriteHalf<TcpStream>,
    tx: LineFramedSink,

    /// 接收外层消息处理
    dispatcher_service: Address<DispatcherService>,
    // 网关管理服务
    // gateway_management_service: Address<GatewayManagement>,
}

impl Actor for GatewayClient {}

impl GatewayClient {
    pub async fn new(
        dispatcher_service: Address<DispatcherService>,
        address: String,
    ) -> Result<Self> {
        let stream = TcpStream::connect(&address).await?;
        let frame = Framed::new(stream, RstRespCodec);
        let (mut fram_write, fram_read) = frame.split::<RstResp>();
        let value = json!({
            "worker_key": "rust_sdk",
            "secret_key": "",
        });
        let mut gateway_event = PHPGatewayProtocol::new();
        gateway_event.cmd = PHPGatewayProtocol::CMD_GATEWAY_CLIENT_CONNECT;
        // gateway_event.cmd = PHPGatewayProtocol::CMD_WORKER_CONNECT;
        let body = ProtocolData::DataStr::<String>(value.to_string());
        gateway_event.set_body(body);
        let event = gateway_event.encode();

        fram_write.send(RstResp::Request(event)).await?;

        let dispatcher_service_cp = dispatcher_service.clone();
        let mv_address = address.clone();
        tokio::spawn(async move {
            Self::gateway_spawn_reader(fram_read, dispatcher_service_cp, mv_address).await
        });

        Ok(GatewayClient {
            status: true,
            address: address,
            tx: fram_write,
            dispatcher_service,
        })
    }

    pub async fn reconnection(&mut self) -> Result<()> {
        // let stream = TcpStream::connect(&self.address).await?;
        // let frame = Framed::new(stream, LinesCodec::new());
        // let (mut fram_write, fram_read) = frame.split::<String>();
        // self.tx = fram_write;

        Ok(())
    }

    pub async fn send(&mut self, data: Vec<u8>) -> Result<()> {
        // let buffer = std::str::from_utf8(data)?;
        self.tx.send(RstResp::Request(data)).await?;
        Ok(())
    }

    pub async fn gateway_spawn_reader(
        // mut reader: ReadHalf<TcpStream>,
        mut reader: LineFramedStream,
        dispatcher_service: Address<DispatcherService>,
        address: String,
    ) {
        // let mut buf = vec![0; 16384];
        loop {
            match reader.next().await {
                None => {
                    let res = GatewayResponse {
                        respond_to: vec![],
                        method: GatewayMethod::Close,
                        address: address.clone(),
                    };
                    match dispatcher_service.send(res).await {
                        Err(e) => {
                            error!("gateway close notification error {:?}", e);
                        }
                        Ok(_) => (),
                    }
                    info!("gateway close");
                    break;
                }
                Some(Err(e)) => {
                    let res = GatewayResponse {
                        respond_to: vec![],
                        method: GatewayMethod::Close,
                        address: address.clone(),
                    };
                    match dispatcher_service.do_send_async(res).await {
                        Err(e) => {
                            error!("gateway close notification error {:?}", e);
                        }
                        Ok(_) => (),
                    }
                    return;
                }
                Some(Ok(buffer)) => {
                    debug!("gateway client send {:?}", buffer);
                    match buffer {
                        RstResp::Request(_) => {}
                        RstResp::Response(data) => {
                            let res = GatewayResponse {
                                respond_to: data,
                                method: GatewayMethod::Send,
                                address: address.clone(),
                            };
                            let a = dispatcher_service.send(res).await;
                        }
                    };
                }
            }
        }
    }

    /*pub async fn gateway_spawn_read_(
        mut rx: ReadHalf<TcpStream>,
        // mut rx: LineFramedStream,
        dispatcher_service: Address<DispatcherService>,
        address: String,
    ) {
        let mut buf = vec![0; 1048576];
        loop {
            match rx.read(&mut buf[..]).await {
                Ok(0) => {
                    let res = GatewayResponse {
                        respond_to: vec![],
                        method: GatewayMethod::Close,
                        address: address.clone(),
                    };
                    let _ = dispatcher_service.do_send_async(res).await;
                    break;
                }
                Ok(n) => {
                    let buffer = &buf[..n];

                    let res = GatewayResponse {
                        respond_to: buffer.to_vec(),
                        method: GatewayMethod::Send,
                        address: address.clone(),
                    };
                    let _ = dispatcher_service.do_send_async(res).await;
                }
                Err(_) => {
                    let res = GatewayResponse {
                        respond_to: vec![],
                        method: GatewayMethod::Close,
                        address: address.clone(),
                    };
                    let _ = dispatcher_service.do_send_async(res).await;
                    break;
                }
            }
        }
    }*/
}

#[derive(Debug)]
pub enum RstResp {
    Request(Vec<u8>),
    Response(Vec<u8>),
}

pub struct RstRespCodec;
impl RstRespCodec {
    /// 最多传送1G数据
    const MAX_SIZE: usize = 1024 * 1024 * 1024 * 8;
}

impl Encoder<RstResp> for RstRespCodec {
    type Error = bincode::Error;
    // 本示例中使用bincode将RstResp转换为&[u8]，也可以使用serde_json::to_vec()，前者效率更高一些
    fn encode(&mut self, item: RstResp, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RstResp::Request(data) => {
                dst.extend_from_slice(&data);
            }
            _ => {}
        }
        Ok(())
    }
}

impl Decoder for RstRespCodec {
    type Item = RstResp;
    type Error = std::io::Error;
    // 从不断被填充的Bytes buf中读取数据，并将其转换到目标类型
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let buf_len = src.len();
        let a = src.to_vec();
        // 如果buf中的数据量连长度声明的大小都不足，则先跳过等待后面更多数据的到来

        if buf_len < 4 {
            return Ok(None);
        }

        let buf = src.split_to(a.len());
        return Ok(Some(RstResp::Response(buf.to_vec())));

        // 先读取帧首，获得声明的帧中实际数据大小
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let data_len = u32::from_be_bytes(length_bytes) as usize;
        if data_len > Self::MAX_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of length {} is too large.", data_len),
            ));
        }

        // 帧的总长度为 4 + frame_len
        let frame_len = data_len + 4;

        // buf中数据量不够，跳过，并预先申请足够的空闲空间来存放该帧后续到来的数据
        if buf_len < frame_len {
            src.reserve(frame_len - buf_len);
            return Ok(None);
        }

        // 数据量足够了，从buf中取出数据转编成帧，并转换为指定类型后返回
        // 需同时将buf截断(split_to会截断)
        let frame_bytes = src.split_to(frame_len);
        return Ok(Some(RstResp::Response(frame_bytes.to_vec())));
    }
}
