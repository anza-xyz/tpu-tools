use {
    crate::{
        error::RateLatencyToolError,
        run_rate_latency_tool_scheduler::{LeaderSlotEstimator, LeaderUpdaterWithSlot},
    },
    async_trait::async_trait,
    log::error,
    solana_clock::Slot,
    solana_clock::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_connection_cache::connection_cache::Protocol,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_tpu_client::nonblocking::tpu_client::LeaderTpuService,
    solana_tpu_client_next::leader_updater::LeaderUpdater,
    std::net::SocketAddr,
    std::sync::atomic::AtomicBool,
    std::sync::atomic::Ordering,
    std::sync::Arc,
};

pub async fn create_leader_updater(
    rpc_client: Arc<RpcClient>,
    websocket_url: String,
) -> Result<Box<dyn LeaderUpdaterWithSlot>, RateLatencyToolError> {
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
    fn estimated_current_slot(&mut self) -> Slot {
        self.leader_tpu_service.estimated_current_slot()
    }
}
