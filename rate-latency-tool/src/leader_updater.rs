use {
    crate::{
        error::RateLatencyToolError,
        run_rate_latency_tool_scheduler::{LeaderSlotEstimator, LeaderUpdaterWithSlot},
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
    Legacy,
    LeaderTracker(LeaderTpuCacheServiceConfig),
    YellowstoneLeaderTracker(LeaderTpuCacheServiceConfig),
}

pub async fn create_leader_updater(
    rpc_client: Arc<RpcClient>,
    websocket_url: String,
    yellowstone_url: Option<String>,
    updater_type: LeaderUpdaterType,
    cancel: CancellationToken,
) -> Result<Box<dyn LeaderUpdaterWithSlot>, RateLatencyToolError> {
    match updater_type {
        LeaderUpdaterType::Pinned(pinned_address) => {
            return Ok(Box::new(PinnedLeaderUpdater {
                address: vec![pinned_address],
            }))
        }
        LeaderUpdaterType::Legacy => {
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
        LeaderUpdaterType::LeaderTracker(config) => {
            let leader_tpu_service =
                NodeAddressService::run(rpc_client, &websocket_url, config, cancel).await?;
            Ok(Box::new(leader_tpu_service))
        }
        LeaderUpdaterType::YellowstoneLeaderTracker(config) => {
            assert!(
                yellowstone_url.is_some(),
                "Yellowstone URL should be specified for yellowstone leader tracker"
            );
            let leader_tpu_service = YellowstoneNodeAddressService::run(
                rpc_client,
                yellowstone_url.unwrap(),
                config,
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
