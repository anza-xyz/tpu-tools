//! Leader updater construction for TPU tools.
//!
//! The factory in this module adapts command-line leader tracker selection into
//! `solana-tpu-client-next` leader updater implementations.

use {
    crate::{
        cli::LeaderTracker,
        custom_geyser_node_address_service::{
            CustomGeyserNodeAddressService, Error as CustomGeyserNodeAddressServiceError,
        },
        yellowstone_leader_tracker::{
            Error as YellowstoneNodeAddressServiceError, YellowstoneNodeAddressService,
        },
    },
    async_trait::async_trait,
    log::{debug, error},
    solana_clock::{NUM_CONSECUTIVE_LEADER_SLOTS, Slot},
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
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    },
    thiserror::Error,
    tokio_util::sync::CancellationToken,
};

/// Provides the current slot estimate alongside upcoming leader addresses.
pub trait LeaderSlotEstimator {
    /// Returns the best available current slot estimate.
    fn get_current_slot(&mut self) -> Slot;
}

/// Leader updater trait object used by TPU tools.
///
/// This combines `solana-tpu-client-next` leader address lookup with a current
/// slot estimate.
pub trait LeaderUpdaterWithSlot: LeaderUpdater + LeaderSlotEstimator {}
impl<T> LeaderUpdaterWithSlot for T where T: LeaderUpdater + LeaderSlotEstimator {}

/// Internal leader updater configuration variants.
///
/// The command-line path normally uses [`create_leader_updater`] with
/// [`LeaderTracker`] instead.
pub enum LeaderUpdaterType {
    /// Always returns a fixed TPU address.
    Pinned(SocketAddr),
    /// Uses the legacy websocket TPU leader service.
    Legacy(String),
    /// Uses the websocket node-address service.
    LeaderTracker((String, LeaderTpuCacheServiceConfig)),
    /// Uses Yellowstone gRPC slot updates.
    YellowstoneLeaderTracker((String, Option<String>, LeaderTpuCacheServiceConfig)),
    /// Uses a custom UDP/Geyser slot updater.
    SlotUpdaterTracker((SocketAddr, LeaderTpuCacheServiceConfig)),
}

#[derive(Debug, Error)]
pub enum Error {
    /// Websocket node-address service failed.
    #[error(transparent)]
    WebsocketNodeAddressServiceError(#[from] WebsocketNodeAddressServiceError),

    /// Yellowstone node-address service failed.
    #[error(transparent)]
    NodeAddressServiceError(#[from] YellowstoneNodeAddressServiceError),

    /// Custom Geyser node-address service failed.
    #[error(transparent)]
    CustomGeyserNodeAddressServiceError(#[from] CustomGeyserNodeAddressServiceError),

    /// Legacy leader updater failed during startup.
    #[error("Legacy leader updater failed to start")]
    LegacyLeaderUpdaterInitializationFailed,
}

/// Creates a leader updater from CLI selection and node-address-service config.
///
/// `websocket_url` is used by websocket-backed modes. Pinned and Yellowstone
/// modes ignore it.
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
