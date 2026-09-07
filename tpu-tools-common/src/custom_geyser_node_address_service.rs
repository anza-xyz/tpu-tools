use {
    async_stream::stream,
    futures::Stream,
    log::*,
    serde::{Deserialize, Deserializer},
    solana_clock::Slot,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_tpu_client_next::node_address_service::{
        LeaderTpuCacheServiceConfig, NodeAddressProvider, NodeAddressService,
        NodeAddressServiceError, SlotEvent,
    },
    std::{net::SocketAddr, sync::Arc},
    thiserror::Error,
    tokio::net::UdpSocket,
    tokio_util::sync::CancellationToken,
};

pub struct CustomGeyserNodeAddressService(NodeAddressService);

#[derive(Debug, Error)]
pub enum Error {
    #[error("UDP Socket Initialization Failed")]
    UdpSocketInitializationFailed,

    #[error(transparent)]
    NodeAddressServiceError(#[from] NodeAddressServiceError),
}

impl CustomGeyserNodeAddressService {
    pub async fn run(
        rpc_client: Arc<RpcClient>,
        bind_address: SocketAddr,
        config: LeaderTpuCacheServiceConfig,
        cancel: CancellationToken,
    ) -> Result<(NodeAddressProvider, Self), Error> {
        #[allow(clippy::disallowed_methods)]
        let socket = UdpSocket::bind(bind_address)
            .await
            .map_err(|_e| Error::UdpSocketInitializationFailed)?;
        let stream = udp_slot_event_stream(socket);

        let (provider, service) =
            NodeAddressService::run(rpc_client, stream, config, cancel).await?;

        Ok((provider, Self(service)))
    }

    pub async fn shutdown(&mut self) -> Result<(), Error> {
        self.0.shutdown().await?;
        Ok(())
    }
}

fn udp_slot_event_stream(socket: UdpSocket) -> impl Stream<Item = SlotEvent> + Send + 'static {
    stream! {
        let mut buf = vec![0u8; 2048];

        loop {
            match socket.recv_from(&mut buf).await {
                Ok((len, from)) => {
                    let timestamp = solana_time_utils::timestamp();
                    let data = &buf[..len];
                    match serde_json::from_slice::<SlotMessage>(data) {
                        Ok(msg) => {
                            trace!("Received SlotMessage from {from}: {msg:?}");
                            match msg.status {
                                SlotStatus::FirstShredReceived => yield SlotEvent::Start { slot: msg.slot, timestamp },
                                SlotStatus::Completed => yield SlotEvent::End { slot: msg.slot, timestamp },
                                _ => continue,
                            };
                        }
                        Err(e) => error!("Failed to parse SlotMessage from {from}: {e}"),
                    }
                }
                Err(e) => {
                    error!("UDP receive failed: {e}");
                    break;
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct SlotMessage {
    pub slot: Slot,
    pub parent: Option<Slot>,
    pub status: SlotStatus,
    pub dead_error: Option<String>,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum SlotStatus {
    Processed,
    Rooted,
    Confirmed,
    FirstShredReceived,
    Completed,
    CreatedBank,
    Dead(String),
}

impl<'de> Deserialize<'de> for SlotStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "processed" => Ok(SlotStatus::Processed),
            "rooted" => Ok(SlotStatus::Rooted),
            "confirmed" => Ok(SlotStatus::Confirmed),
            "first_shred_received" => Ok(SlotStatus::FirstShredReceived),
            "completed" => Ok(SlotStatus::Completed),
            "created_bank" => Ok(SlotStatus::CreatedBank),
            "dead" => Ok(SlotStatus::Dead("dead".to_string())),
            _ => Err(serde::de::Error::unknown_variant(
                &s,
                &[
                    "processed",
                    "rooted",
                    "confirmed",
                    "first_shred_received",
                    "completed",
                    "created_bank",
                    "dead",
                ],
            )),
        }
    }
}
