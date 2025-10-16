use {
    crate::run_rate_latency_tool_scheduler::LeaderSlotEstimator,
    log::*,
    node_address_service::{
        leader_tpu_cache_service::LeaderUpdateReceiver,
        websocket_slot_update_service::{RecentLeaderSlots, SlotEstimator},
        LeaderTpuCacheService, LeaderTpuCacheServiceConfig, NodeAddressServiceError, SlotReceiver,
    },
    serde::{Deserialize, Deserializer},
    solana_clock::Slot,
    solana_commitment_config::CommitmentConfig,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_time_utils::timestamp,
    solana_tpu_client_next::leader_updater::LeaderUpdater,
    std::{net::SocketAddr, sync::Arc},
    tokio::{net::UdpSocket, sync::watch, task::JoinHandle},
    tokio_util::sync::CancellationToken,
    tonic::async_trait,
};

pub struct SlotUpdaterNodeAddressService {
    leaders_receiver: LeaderUpdateReceiver,
    slot_receiver: SlotReceiver,
    slot_update_service: SlotUpdateService,
    leader_cache_service: LeaderTpuCacheService,
}

impl SlotUpdaterNodeAddressService {
    pub async fn run(
        rpc_client: Arc<RpcClient>,
        bind_address: SocketAddr,
        config: LeaderTpuCacheServiceConfig,
        cancel: CancellationToken,
    ) -> Result<Self, NodeAddressServiceError> {
        let start_slot = rpc_client
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await?;

        let (slot_receiver, slot_update_service) =
            SlotUpdateService::run(start_slot, bind_address, cancel.clone()).await?;
        let (leaders_receiver, leader_cache_service) =
            LeaderTpuCacheService::run(rpc_client, slot_receiver.clone(), config, cancel).await?;

        Ok(Self {
            leaders_receiver,
            slot_receiver,
            slot_update_service,
            leader_cache_service,
        })
    }

    pub async fn shutdown(self) -> Result<(), NodeAddressServiceError> {
        self.slot_update_service.shutdown().await?;
        self.leader_cache_service.shutdown().await?;
        Ok(())
    }
}

#[async_trait]
impl LeaderUpdater for SlotUpdaterNodeAddressService {
    fn next_leaders(&mut self, _lookahead_leaders: usize) -> Vec<SocketAddr> {
        self.leaders_receiver.leaders()
    }

    async fn stop(&mut self) {}
}

#[async_trait]
impl LeaderSlotEstimator for SlotUpdaterNodeAddressService {
    fn get_current_slot(&mut self) -> Slot {
        self.slot_receiver.slot_with_timestamp().0
    }
}

pub struct SlotUpdateService {
    handle: JoinHandle<Result<(), NodeAddressServiceError>>,
    cancel: CancellationToken,
}

impl SlotUpdateService {
    pub async fn run(
        current_slot: Slot,
        bind_address: SocketAddr,
        cancel: CancellationToken,
    ) -> Result<(SlotReceiver, Self), NodeAddressServiceError> {
        let mut recent_slots = RecentLeaderSlots::new();
        let (slot_sender, slot_receiver) = watch::channel((current_slot, timestamp()));
        let slot_receiver_clone = slot_receiver.clone();
        let cancel_clone = cancel.clone();

        let socket = UdpSocket::bind(bind_address)
            .await
            .map_err(|_e| NodeAddressServiceError::InitializationFailed)?;

        let main_loop = async move {
            let mut buf = vec![0u8; 2048];
            loop {
                tokio::select! {
                    res = socket.recv_from(&mut buf) => {
                        match res {
                            Ok((len, from)) => {
                                let data = &buf[..len];
                                match serde_json::from_slice::<SlotMessage>(data) {
                                    Ok(msg) => {
                                        trace!("Received SlotMessage from {from}: {:?}", msg);
                                        let current_slot = match msg.status {
                                            SlotStatus::FirstShredReceived => msg.slot,
                                            SlotStatus::Completed => msg.slot.saturating_add(1),
                                            _ => continue,
                                        };
                                        recent_slots.record_slot(current_slot);
                                        let cached_estimated_slot = slot_receiver.borrow().0;
                                        let estimated_slot = recent_slots.estimate_current_slot();
                                        debug!("Received slot update: {}, {}, {:?}", msg.created_at, msg.slot, msg.status);
                                        debug!("slot received: {}, status: {:?}, estimated_slot: {}, timestamp: {}",
                                            msg.slot, msg.status, estimated_slot, msg.created_at);
                                        if cached_estimated_slot < estimated_slot {
                                            slot_sender.send((estimated_slot, msg.created_at))
                                                .expect("Failed to send slot update");
                                        }
                                     }
                                    Err(e) => {
                                        error!("Failed to parse SlotMessage from {from}: {e}");
                                    }
                                }
                            }
                            Err(e) => {
                                error!("UDP receive failed: {e}");
                            }
                        }
                    }
                    _ = cancel.cancelled() => {
                        info!("LeaderTracker cancelled, exiting slot watcher.");
                        break;
                    }
                }
            }
            Ok(())
        };

        let handle = tokio::spawn(main_loop);

        Ok((
            SlotReceiver::new(slot_receiver_clone),
            Self {
                handle,
                cancel: cancel_clone,
            },
        ))
    }

    pub async fn shutdown(self) -> Result<(), NodeAddressServiceError> {
        self.cancel.cancel();
        self.handle.await??;
        Ok(())
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
