use anyhow::Result;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::thread::sleep;
use tokio::io::{self, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;
use tokio_util::codec::{Framed, LinesCodec};
use xtra::{Actor, Address};

use crate::gateway_management::GatewayManagement;
use crate::types::*;

type LineFramedStream = SplitStream<Framed<TcpStream, LinesCodec>>;
type LineFramedSink = SplitSink<Framed<TcpStream, LinesCodec>, String>;

pub struct RegisterClient {
    address: String,

    pub register_status: bool,

    // register_write: WriteHalf<TcpStream>,
    register_write: LineFramedSink,
    gm_service: Address<GatewayManagement>,
}

impl RegisterClient {
    pub async fn new(address: String, gm_service: Address<GatewayManagement>) -> Result<Self> {
        let mut socket = TcpStream::connect(&address).await?;
        let framed = Framed::new(socket, LinesCodec::new());
        // let (mut gateway_rd, mut wr) = io::split(socket);
        let (mut slnk, stream) = framed.split::<String>();
        let event = "{\"event\":\"worker_connect\",\"secret_key\":\"\"}".to_string();
        // let _ = wr.write_all(event.as_bytes()).await.unwrap();

        slnk.send(event).await?;
        let dispatcher_service_cp = gm_service.clone();
        tokio::spawn(
            async move { Self::gateway_spawn_reader(stream, dispatcher_service_cp).await },
        );

        Ok(RegisterClient {
            address: address,
            register_status: true,
            register_write: slnk,
            gm_service,
        })
    }

    /// 注册中心连接
    pub async fn register_conn(&mut self) -> Result<()> {
        // let mut socket = TcpStream::connect(&self.address).await?;
        // let (mut gateway_rd, mut wr) = io::split(socket);
        // let event = "{\"event\":\"worker_connect\",\"secret_key\":\"\"}\n".to_string();
        // let _ = wr.write_all(event.as_bytes()).await?;
        // self.register_status = true;
        // let gm_service = self.gm_service.clone();
        // tokio::spawn(async move { Self::gateway_spawn_reader(gateway_rd, gm_service).await });
        Ok(())
    }

    pub async fn gateway_tryconnection(&mut self) -> Result<()> {
        match TcpStream::connect("127.0.0.1:1238").await {
            Ok(socket) => {
                // let (mut gateway_rd, mut wr) = io::split(socket);
                // let event = "{\"event\":\"worker_connect\",\"secret_key\":\"\"}\n".to_string();
                // wr.write(event.as_bytes()).await?;
                // self.register_status = true;
                // self.register_write = wr;
                // let gm_service = self.gm_service.clone();
                // tokio::spawn(
                //     async move { Self::gateway_spawn_reader(gateway_rd, gm_service).await },
                // );

                let framed = Framed::new(socket, LinesCodec::new());
                let (mut slnk, gateway_rd) = framed.split::<String>();
                let event = "{\"event\":\"worker_connect\",\"secret_key\":\"\"}".to_string();
                slnk.send(event).await?;
                self.register_status = true;
                self.register_write = slnk;
                let gm_service = self.gm_service.clone();
                tokio::spawn(
                    async move { Self::gateway_spawn_reader(gateway_rd, gm_service).await },
                );
            }
            Err(e) => {
                self.register_status = false;
            }
        }

        Ok(())
    }

    pub async fn gateway_spawn_reader(
        // mut reader: ReadHalf<TcpStream>,
        mut reader: LineFramedStream,
        gm: Address<GatewayManagement>,
    ) {
        let mut buf = vec![0; 16384];
        loop {
            match reader.next().await {
                None => {
                    error!("client closed");
                    //修改注册中心连接状态
                    // 断开tcp发送
                    loop {
                        //先修改状态
                        sleep(Duration::from_secs(5));
                        let stat = {
                            if let Ok(status) = gm.send(RegisterClose("".to_string())).await {
                                status
                            } else {
                                false
                            }
                        };
                        if stat {
                            return;
                        }
                    }
                }
                Some(Err(e)) => {
                    error!("read from client error: {}", e);
                    //修改注册中心连接状态
                    // 断开tcp发送
                    loop {
                        //先修改状态
                        sleep(Duration::from_secs(5));
                        let stat = {
                            if let Ok(status) = gm.send(RegisterClose("".to_string())).await {
                                status
                            } else {
                                false
                            }
                        };
                        if stat {
                            return;
                        }
                    }
                }
                Some(Ok(buffer)) => {
                    debug!("read from client. content: {}", buffer);
                    let _ = gm.do_send_async(RegisterMessage(buffer.into_bytes())).await;
                }
            }

            // match reader.next().await {
            //     // match reader.read(&mut buf[..]).await {
            //     Ok(0) => {
            //         //修改注册中心连接状态
            //         println!("read 0 bytes");
            //         // 断开tcp发送
            //         loop {
            //             //先修改状态
            //             sleep(Duration::from_secs(5));
            //             let stat = {
            //                 if let Ok(status) = gm.send(RegisterClose("".to_string())).await {
            //                     status
            //                 } else {
            //                     false
            //                 }
            //             };
            //             println!("stat {:?}", stat);
            //             if stat {
            //                 return;
            //             }
            //         }
            //     }
            //     Ok(n) => {
            //         let buffer = &buf[..n];
            //         // 接收tcp
            //         println!("read {} bytes: {:?}", n, String::from_utf8_lossy(buffer));
            //         let _ = gm.do_send_async(RegisterMessage(buffer.to_vec())).await;
            //         // return;
            //     }
            //     Err(_) => {
            //         //修改注册中心连接状态
            //         // REGISTER_STATUS.store(false, Ordering::Relaxed);
            //         println!("read error");
            //         loop {
            //             //先修改状态
            //             sleep(Duration::from_secs(5));
            //             let stat = {
            //                 if let Ok(status) = gm.send(RegisterClose("".to_string())).await {
            //                     status
            //                 } else {
            //                     false
            //                 }
            //             };
            //             println!("stat {:?}", stat);
            //             if stat {
            //                 return;
            //             }
            //         }
            //     }
            // }
        }
    }

    pub async fn gateway_spawn_reader_back(
        mut reader: ReadHalf<TcpStream>,
        // mut reader: LineFramedStream,
        gm: Address<GatewayManagement>,
    ) {
        let mut buf = vec![0; 16384];
        loop {
            match reader.read(&mut buf[..]).await {
                // match reader.read(&mut buf[..]).await {
                Ok(0) => {
                    //修改注册中心连接状态
                    println!("read 0 bytes");
                    // 断开tcp发送
                    loop {
                        //先修改状态
                        sleep(Duration::from_secs(5));
                        let stat = {
                            if let Ok(status) = gm.send(RegisterClose("".to_string())).await {
                                status
                            } else {
                                false
                            }
                        };
                        println!("stat {:?}", stat);
                        if stat {
                            return;
                        }
                    }
                }
                Ok(n) => {
                    let buffer = &buf[..n];
                    // 接收tcp
                    println!("read {} bytes: {:?}", n, String::from_utf8_lossy(buffer));
                    let _ = gm.do_send_async(RegisterMessage(buffer.to_vec())).await;
                    // return;
                }
                Err(_) => {
                    //修改注册中心连接状态
                    // REGISTER_STATUS.store(false, Ordering::Relaxed);
                    println!("read error");
                    loop {
                        //先修改状态
                        sleep(Duration::from_secs(5));
                        let stat = {
                            if let Ok(status) = gm.send(RegisterClose("".to_string())).await {
                                status
                            } else {
                                false
                            }
                        };
                        println!("stat {:?}", stat);
                        if stat {
                            return;
                        }
                    }
                }
            }
        }
    }
}
