use {
    crate::{
        error::RateLatencyToolError,
        run_rate_latency_tool_scheduler::{LeaderSlotEstimator, LeaderUpdaterWithSlot},
        slot_updater_node_address_service::SlotUpdaterNodeAddressService,
        yellowstone_leader_tracker::YellowstoneNodeAddressService,
    },
    async_trait::async_trait,
    log::error,
    node_address_service::{LeaderTpuCacheServiceConfig, NodeAddressService},
    solana_clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    solana_connection_cache::connection_cache::Protocol,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_tpu_client::nonblocking::tpu_client::LeaderTpuService,
    solana_tpu_client_next::leader_updater::LeaderUpdater,
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    },
    tokio_util::sync::CancellationToken,
};

pub enum LeaderUpdaterType {
    Pinned(SocketAddr),
    Legacy(String),
    LeaderTracker((String, LeaderTpuCacheServiceConfig)),
    YellowstoneLeaderTracker((String, Option<String>, LeaderTpuCacheServiceConfig)),
    SlotUpdaterTracker((SocketAddr, LeaderTpuCacheServiceConfig)),
}

pub async fn create_leader_updater(
    rpc_client: Arc<RpcClient>,
    updater_type: LeaderUpdaterType,
    cancel: CancellationToken,
) -> Result<Box<dyn LeaderUpdaterWithSlot>, RateLatencyToolError> {
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
                        RateLatencyToolError::LeaderUpdaterError
                    })?;
            Ok(Box::new(LeaderUpdaterService {
                leader_tpu_service,
                exit,
            }))
        }
        LeaderUpdaterType::LeaderTracker((websocket_url, config)) => {
            let leader_tpu_service =
                NodeAddressService::run(rpc_client, &websocket_url, config, cancel).await?;
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
            let leader_tpu_service = SlotUpdaterNodeAddressService::run(
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
impl LeaderSlotEstimator for NodeAddressService {
    fn get_current_slot(&mut self) -> Slot {
        self.estimated_current_slot()
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
