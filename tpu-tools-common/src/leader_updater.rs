//! Leader updater construction for TPU tools.
//!
//! The factory in this module adapts command-line leader tracker selection into
//! cloneable `solana-tpu-client-next` leader updater implementations. For
//! node-address-service based trackers, one background service can feed many
//! updater handles.

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
    log::debug,
    solana_clock::Slot,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_tpu_client_next::{
        leader_updater::{LeaderUpdater, SlotEstimate},
        node_address_service::{LeaderTpuCacheServiceConfig, NodeAddressProvider},
        websocket_node_address_service::{
            Error as WebsocketNodeAddressServiceError, WebsocketNodeAddressService,
        },
    },
    std::{net::SocketAddr, sync::Arc},
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
    /// Uses the websocket node-address service.
    LeaderTracker((String, LeaderTpuCacheServiceConfig)),
    /// Uses Yellowstone gRPC slot updates.
    YellowstoneLeaderTracker((String, Option<String>, LeaderTpuCacheServiceConfig)),
    /// Uses a custom UDP/Geyser slot updater.
    SlotUpdaterTracker((SocketAddr, LeaderTpuCacheServiceConfig)),
}

/// Factory that creates per-client leader updater handles.
///
/// For websocket, Yellowstone, and custom Geyser trackers, this owns one leader
/// update service and clones its provider for every scheduler/client. For
/// pinned leaders no service is needed.
pub enum LeaderUpdaterFactory {
    Pinned {
        address: SocketAddr,
    },
    SharedNodeAddress {
        provider: NodeAddressProvider,
        service: LeaderUpdateService,
    },
}

impl LeaderUpdaterFactory {
    /// Creates a leader updater handle for one TPU client or scheduler.
    pub async fn create_updater(&self) -> Result<Box<dyn LeaderUpdaterWithSlot>, Error> {
        match self {
            Self::Pinned { address } => Ok(Box::new(PinnedLeaderUpdater {
                addresses: vec![*address],
            })),
            Self::SharedNodeAddress { provider, .. } => Ok(Box::new(provider.clone())),
        }
    }

    /// Stops the shared leader update service, if this factory owns one.
    pub async fn shutdown(self) -> Result<(), Error> {
        match self {
            Self::SharedNodeAddress { service, .. } => service.shutdown().await,
            Self::Pinned { .. } => Ok(()),
        }
    }
}

/// Owns a node-address-service based leader update service.
pub enum LeaderUpdateService {
    Websocket(WebsocketNodeAddressService),
    Yellowstone(YellowstoneNodeAddressService),
    CustomGeyser(CustomGeyserNodeAddressService),
}

impl LeaderUpdateService {
    async fn shutdown(self) -> Result<(), Error> {
        match self {
            Self::Websocket(service) => service.shutdown().await.map_err(Error::from),
            Self::Yellowstone(mut service) => service.shutdown().await.map_err(Error::from),
            Self::CustomGeyser(mut service) => service.shutdown().await.map_err(Error::from),
        }
    }
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
}

/// Creates a leader updater factory from CLI selection and node-address-service config.
///
/// `websocket_url` is used by websocket-backed modes. Pinned and Yellowstone
/// modes ignore it.
pub async fn create_leader_updater(
    rpc_client: Arc<RpcClient>,
    leader_tracker: LeaderTracker,
    config: LeaderTpuCacheServiceConfig,
    websocket_url: String,
    cancel: CancellationToken,
) -> Result<LeaderUpdaterFactory, Error> {
    match leader_tracker {
        LeaderTracker::PinnedLeaderTracker { address } => {
            debug!("Using pinned leader updater");
            Ok(LeaderUpdaterFactory::Pinned { address })
        }

        LeaderTracker::WsLeaderTracker => {
            debug!("Using node address service leader tracker updater");
            let (provider, service) =
                WebsocketNodeAddressService::run(rpc_client, websocket_url, config, cancel).await?;
            Ok(LeaderUpdaterFactory::SharedNodeAddress {
                provider,
                service: LeaderUpdateService::Websocket(service),
            })
        }
        LeaderTracker::YellowstoneLeaderTracker { url, token } => {
            debug!("Using yellowstone leader tracker updater");
            let (provider, service) = YellowstoneNodeAddressService::run(
                rpc_client,
                url,
                token.as_deref(),
                config,
                cancel,
            )
            .await?;
            Ok(LeaderUpdaterFactory::SharedNodeAddress {
                provider,
                service: LeaderUpdateService::Yellowstone(service),
            })
        }
        LeaderTracker::CustomLeaderTracker { bind_address } => {
            debug!("Using custom geyser node address service leader tracker updater");
            let (provider, service) =
                CustomGeyserNodeAddressService::run(rpc_client, bind_address, config, cancel)
                    .await?;
            Ok(LeaderUpdaterFactory::SharedNodeAddress {
                provider,
                service: LeaderUpdateService::CustomGeyser(service),
            })
        }
    }
}

struct PinnedLeaderUpdater {
    addresses: Vec<SocketAddr>,
}

impl LeaderUpdater for PinnedLeaderUpdater {
    fn next_leaders(
        &mut self,
        _lookahead_leaders: usize,
        leaders: &mut Vec<SocketAddr>,
    ) -> Option<SlotEstimate> {
        leaders.extend_from_slice(&self.addresses);
        None
    }
}

impl LeaderSlotEstimator for PinnedLeaderUpdater {
    fn get_current_slot(&mut self) -> Slot {
        0
    }
}

impl LeaderSlotEstimator for NodeAddressProvider {
    fn get_current_slot(&mut self) -> Slot {
        self.estimated_current_slot().slot
    }
}
