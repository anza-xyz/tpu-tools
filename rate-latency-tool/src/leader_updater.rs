use {
    crate::{
        run_rate_latency_tool_scheduler::{LeaderSlotEstimator, LeaderUpdaterWithSlot},
        slot_updater_node_address_service::{
            CustomGeyserNodeAddressService, Error as CustomGeyserNodeAddressServiceError,
        },
        yellowstone_leader_tracker::{
            Error as YellowstoneNodeAddressServiceError, YellowstoneNodeAddressService,
        },
    },
    async_trait::async_trait,
    log::error,
    solana_clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    solana_connection_cache::connection_cache::Protocol,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_tpu_client::nonblocking::tpu_client::LeaderTpuService,
    solana_tpu_client_next::{
        leader_updater::LeaderUpdater,
        node_address_service::LeaderTpuCacheServiceConfig,
        websocket_node_address_service::{
            Error as WebsocketNodeAddressServiceError, WebsocketNodeAddressService,
        },
    },
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    },
    thiserror::Error,
    tokio_util::sync::CancellationToken,
};

pub enum LeaderUpdaterType {
    Pinned(SocketAddr),
    Legacy(String),
    LeaderTracker((String, LeaderTpuCacheServiceConfig)),
    YellowstoneLeaderTracker((String, Option<String>, LeaderTpuCacheServiceConfig)),
    SlotUpdaterTracker((SocketAddr, LeaderTpuCacheServiceConfig)),
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    WebsocketNodeAddressServiceError(#[from] WebsocketNodeAddressServiceError),

    #[error(transparent)]
    NodeAddressServiceError(#[from] YellowstoneNodeAddressServiceError),

    #[error(transparent)]
    CustomGeyserNodeAddressServiceError(#[from] CustomGeyserNodeAddressServiceError),

    #[error("Legacy leader updater failed to start")]
    LegacyLeaderUpdaterInitializationFailed,
}

pub async fn create_leader_updater(
    rpc_client: Arc<RpcClient>,
    updater_type: LeaderUpdaterType,
    cancel: CancellationToken,
) -> Result<Box<dyn LeaderUpdaterWithSlot>, Error> {
    match updater_type {
        LeaderUpdaterType::Pinned(pinned_address) => Ok(Box::new(PinnedLeaderUpdater {
            address: vec![pinned_address],
        })),
        LeaderUpdaterType::Legacy(websocket_url) => {
            let exit = Arc::new(AtomicBool::new(false));
            let leader_tpu_service =
                LeaderTpuService::new(rpc_client, &websocket_url, Protocol::QUIC, exit.clone())
                    .await
                    .map_err(|error| {
                        error!("Failed to create a LeaderTpuService: {error}");
                        Error::LegacyLeaderUpdaterInitializationFailed
                    })?;
            Ok(Box::new(LeaderUpdaterService {
                leader_tpu_service,
                exit,
            }))
        }
        LeaderUpdaterType::LeaderTracker((websocket_url, config)) => {
            let leader_tpu_service =
                WebsocketNodeAddressService::run(rpc_client, websocket_url, config, cancel).await?;
            Ok(Box::new(leader_tpu_service))
        }
        LeaderUpdaterType::YellowstoneLeaderTracker((
            yellowstone_url,
            yellowstone_token,
            config,
        )) => {
            let leader_tpu_service = YellowstoneNodeAddressService::run(
                rpc_client,
                yellowstone_url,
                yellowstone_token.as_deref(),
                config,
                cancel,
            )
            .await?;
            Ok(Box::new(leader_tpu_service))
        }
        LeaderUpdaterType::SlotUpdaterTracker((bind, leader_tpu_cache_service_config)) => {
            let leader_tpu_service = CustomGeyserNodeAddressService::run(
                rpc_client,
                bind,
                leader_tpu_cache_service_config,
                cancel,
            )
            .await?;
            Ok(Box::new(leader_tpu_service))
        }
    }
}

/// `LeaderUpdaterService` is an implementation of the [`LeaderUpdater`] trait
/// that dynamically retrieves the current and upcoming leaders by communicating
/// with the Solana network using [`LeaderTpuService`].
struct LeaderUpdaterService {
    leader_tpu_service: LeaderTpuService,
    exit: Arc<AtomicBool>,
}

#[async_trait]
impl LeaderUpdater for LeaderUpdaterService {
    fn next_leaders(&mut self, lookahead_leaders: usize) -> Vec<SocketAddr> {
        let lookahead_slots =
            (lookahead_leaders as u64).saturating_mul(NUM_CONSECUTIVE_LEADER_SLOTS);
        self.leader_tpu_service.leader_tpu_sockets(lookahead_slots)
    }

    async fn stop(&mut self) {
        self.exit.store(true, Ordering::Relaxed);
        self.leader_tpu_service.join().await;
    }
}

#[async_trait]
impl LeaderSlotEstimator for LeaderUpdaterService {
    fn get_current_slot(&mut self) -> Slot {
        self.leader_tpu_service.estimated_current_slot()
    }
}

#[async_trait]
impl LeaderSlotEstimator for WebsocketNodeAddressService {
    fn get_current_slot(&mut self) -> Slot {
        self.current_slot()
    }
}

struct PinnedLeaderUpdater {
    address: Vec<SocketAddr>,
}

#[async_trait]
impl LeaderUpdater for PinnedLeaderUpdater {
    fn next_leaders(&mut self, _lookahead_leaders: usize) -> Vec<SocketAddr> {
        self.address.clone()
    }

    async fn stop(&mut self) {}
}

#[async_trait]
impl LeaderSlotEstimator for PinnedLeaderUpdater {
    fn get_current_slot(&mut self) -> Slot {
        0
    }
}
