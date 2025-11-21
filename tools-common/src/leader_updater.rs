use {
    crate::{
        cli::LeaderTracker,
        custom_geyser_node_address_service::{
            CustomGeyserNodeAddressService, Error as CustomGeyserNodeAddressServiceError,
        },
    },
    async_trait::async_trait,
    log::{debug, error},
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
    tpu_client_next_extensions::yellowstone_leader_tracker::{
        Error as YellowstoneNodeAddressServiceError, YellowstoneNodeAddressService,
    },
};

pub trait LeaderSlotEstimator {
    fn get_current_slot(&mut self) -> Slot;
}

pub trait LeaderUpdaterWithSlot: LeaderUpdater + LeaderSlotEstimator {}
impl<T> LeaderUpdaterWithSlot for T where T: LeaderUpdater + LeaderSlotEstimator {}

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
    leader_tracker: LeaderTracker,
    config: LeaderTpuCacheServiceConfig,
    websocket_url: String,
    cancel: CancellationToken,
) -> Result<Box<dyn LeaderUpdaterWithSlot>, Error> {
    match leader_tracker {
        LeaderTracker::PinnedLeaderTracker { address } => {
            debug!("Using pinned leader updater");
            Ok(Box::new(PinnedLeaderUpdater {
                address: vec![address],
            }))
        }
        LeaderTracker::LegacyLeaderTracker => {
            debug!("Using legacy leader updater");

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
        LeaderTracker::WsLeaderTracker => {
            debug!("Using node address service leader tracker updater");
            let leader_tpu_service =
                WebsocketNodeAddressService::run(rpc_client, websocket_url, config, cancel).await?;
            Ok(Box::new(leader_tpu_service))
        }
        LeaderTracker::YellowstoneLeaderTracker { url, token } => {
            debug!("Using yellowstone leader tracker updater");
            let leader_tpu_service = YellowstoneNodeAddressService::run(
                rpc_client,
                url,
                token.as_deref(),
                config,
                cancel,
            )
            .await?;
            Ok(Box::new(leader_tpu_service))
        }
        LeaderTracker::CustomLeaderTracker { bind_address } => {
            debug!("Using custom geyser node address service leader tracker updater");
            let leader_tpu_service =
                CustomGeyserNodeAddressService::run(rpc_client, bind_address, config, cancel)
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

#[async_trait]
impl LeaderSlotEstimator for YellowstoneNodeAddressService {
    fn get_current_slot(&mut self) -> Slot {
        self.0.estimated_current_slot()
    }
}
