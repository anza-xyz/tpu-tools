use {
    crate::{
        run_rate_latency_tool_scheduler::LeaderSlotEstimator,
        yellowstone_subscriber::{create_client_config, create_geyser_client},
    },
    futures_util::stream::StreamExt,
    log::*,
    node_address_service::{LeaderTpuCacheServiceConfig, NodeAddressServiceError},
    solana_clock::{Slot, DEFAULT_MS_PER_SLOT, NUM_CONSECUTIVE_LEADER_SLOTS},
    solana_commitment_config::CommitmentConfig,
    solana_pubkey::Pubkey,
    solana_quic_definitions::QUIC_PORT_OFFSET,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_rpc_client_api::response::RpcContactInfo,
    solana_time_utils::timestamp,
    solana_tpu_client_next::leader_updater::LeaderUpdater,
    std::{
        collections::{HashMap, VecDeque},
        net::SocketAddr,
        str::FromStr,
        sync::Arc,
    },
    tokio::{
        sync::watch,
        task::JoinHandle,
        time::{interval, Duration, Instant},
    },
    tokio_util::sync::CancellationToken,
    tonic::async_trait,
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel, SlotStatus, SubscribeRequest,
        SubscribeRequestFilterSlots, SubscribeUpdateSlot,
    },
};

/// Maximum number of slots used to build TPU socket fanout set
pub const MAX_FANOUT_SLOTS: u64 = 100;

/// Convinience wrapper for WebsocketSlotUpdateService and LeaderTpuCacheService
/// to track upcoming leaders and maintains an up-to-date mapping of leader id
/// to TPU socket address.
pub struct YellowstoneNodeAddressService {
    slot_receiver: watch::Receiver<(Slot, u64, Duration)>,
    leaders_receiver: watch::Receiver<(Slot, Vec<SocketAddr>)>,
    slot_watcher_service_handle: JoinHandle<Result<(), NodeAddressServiceError>>,
    leader_tpu_service_handle: JoinHandle<Result<(), NodeAddressServiceError>>,
    cancel: CancellationToken,
}

impl YellowstoneNodeAddressService {
    pub async fn run(
        rpc_client: Arc<RpcClient>,
        yellowstone_url: String,
        config: LeaderTpuCacheServiceConfig,
        cancel: CancellationToken,
    ) -> Result<Self, NodeAddressServiceError> {
        let start_slot = rpc_client
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await?;

        let estimated_slot_duration = Duration::from_millis(DEFAULT_MS_PER_SLOT);
        let (slot_sender, slot_receiver) =
            watch::channel((start_slot, timestamp(), estimated_slot_duration));
        let (leaders_sender, leaders_receiver) =
            watch::channel::<(Slot, Vec<SocketAddr>)>((start_slot, vec![]));
        let slot_watcher_service_handle = tokio::spawn(YellowstoneSlotUpdateService::run(
            start_slot,
            slot_sender,
            slot_receiver.clone(),
            yellowstone_url,
            cancel.clone(),
        ));
        let leader_cache_service = LeaderTpuCacheService::new(
            rpc_client.clone(),
            slot_receiver.clone(),
            leaders_sender,
            config,
            cancel.clone(),
        )
        .await?;
        let leader_tpu_service_handle = tokio::spawn(leader_cache_service.run());

        Ok(Self {
            slot_receiver,
            leaders_receiver,
            slot_watcher_service_handle,
            leader_tpu_service_handle,
            cancel,
        })
    }

    pub async fn join(self) -> Result<(), NodeAddressServiceError> {
        self.slot_watcher_service_handle.await??;
        self.leader_tpu_service_handle.await??;

        Ok(())
    }
}

#[async_trait]
impl LeaderUpdater for YellowstoneNodeAddressService {
    //TODO(klykov): we need to consier removing next leaders with lookahead_leaders in the future
    fn next_leaders(&mut self, _lookahead_leaders: usize) -> Vec<SocketAddr> {
        self.leaders_receiver.borrow().1.clone()
    }

    //TODO(klykov): stop should return error to handle join and also stop should
    //consume the object.
    async fn stop(&mut self) {
        self.cancel.cancel();
    }
}

#[async_trait]
impl LeaderSlotEstimator for YellowstoneNodeAddressService {
    fn get_current_slot(&mut self) -> Slot {
        self.slot_receiver.borrow().0
    }
}

pub struct YellowstoneSlotUpdateService;

impl YellowstoneSlotUpdateService {
    pub async fn run(
        start_slot: Slot,
        slot_sender: watch::Sender<(Slot, u64, Duration)>,
        slot_receiver: watch::Receiver<(Slot, u64, Duration)>,
        yellowstone_url: String,
        cancel: CancellationToken,
    ) -> Result<(), NodeAddressServiceError> {
        assert!(
            !yellowstone_url.is_empty(),
            "Yellowstone URL must not be empty"
        );
        let client_config = create_client_config(&yellowstone_url);
        let mut client = create_geyser_client(client_config).await.map_err(|e| {
            error!("Failed to create Yellowstone client: {e:?}");
            NodeAddressServiceError::Other
        })?;

        let request = Self::build_request();

        let mut recent_slots = RecentLeaderSlots::new(start_slot);

        // Track the last time a slot update was received. In case of current leader is not sending relevant shreds for some reason, the current slot will not update.
        let mut last_slot_time = Instant::now();
        const FALLBACK_SLOT_TIMEOUT: Duration = Duration::from_millis(DEFAULT_MS_PER_SLOT);

        let mut interval = interval(FALLBACK_SLOT_TIMEOUT);
        let (_subscribe_tx, mut stream) = client
            .subscribe_with_request(Some(request))
            .await
            .map_err(|e| {
                error!("Failed to subscribe to Yellowstone: {e:?}");
                NodeAddressServiceError::Other
            })?;

        loop {
            tokio::select! {
            // biased to always prefer slot update over fallback slot injection
            biased;
            maybe_update = stream.next() => {
                if let Some(Ok(update)) = maybe_update {
                    match update.update_oneof.expect("Should be valid message") {
                        UpdateOneof::Slot(SubscribeUpdateSlot { slot, status, .. }) => {
                            let current_slot = match SlotStatus::try_from(status).expect("Should be valid status code") {
                                // This update indicates that we have just received the first shred from
                                // the leader for this slot and they are probably still accepting transactions.
                                SlotStatus::SlotFirstShredReceived => slot,
                                //TODO(klykov): fall back on bank created to use with solana test validator
                                // This update indicates that a full slot was received by the connected
                                // node so we can stop sending transactions to the leader for that slot
                                //SlotStatus::SlotCompleted  => slot.saturating_add(1),
                                SlotStatus::SlotCreatedBank => slot,
                                _ => continue,
                            };
                            recent_slots.record_slot(current_slot);
                            last_slot_time = Instant::now();
                            let cached_estimated_slot = slot_receiver.borrow().0;
                            let estimated_slot = recent_slots.estimate_current_slot();
                            info!("slot received: {slot}, status: {status}, estimated_slot: {estimated_slot}");
                            if cached_estimated_slot < estimated_slot {
                                slot_sender.send((estimated_slot, timestamp(), Duration::from_millis(DEFAULT_MS_PER_SLOT)))
                                    .expect("Failed to send slot update");
                            }
                        }
                        _ => {
                            error!("Unexpected update type received from Yellowstone");
                        }
                    }
                }
            }

            _ = interval.tick(), if last_slot_time.elapsed() > FALLBACK_SLOT_TIMEOUT => {
                let estimated = recent_slots.estimate_current_slot().saturating_add(1);
                info!("Injecting fallback slot {estimated}");
                recent_slots.record_slot(estimated);
                recent_slots.record_slot(estimated);
                last_slot_time = Instant::now();
            }

            _ = cancel.cancelled() => {
                info!("LeaderTracker cancelled, exiting slot watcher.");
                break;
            }
            }
        }

        Ok(())
    }

    fn build_request() -> SubscribeRequest {
        let slots = HashMap::from([(
            "client".to_string(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        )]);

        SubscribeRequest {
            accounts: HashMap::new(),
            slots,
            transactions: HashMap::new(),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: Some(CommitmentLevel::Processed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        }
    }
}

pub struct LeaderTpuCacheService {
    rpc_client: Arc<RpcClient>,
    config: LeaderTpuCacheServiceConfig,
    slot_receiver: watch::Receiver<(Slot, u64, Duration)>,
    leaders_sender: watch::Sender<(Slot, Vec<SocketAddr>)>,
    cancel: CancellationToken,
}

impl LeaderTpuCacheService {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        slot_receiver: watch::Receiver<(Slot, u64, Duration)>,
        leaders_sender: watch::Sender<(Slot, Vec<SocketAddr>)>,
        config: LeaderTpuCacheServiceConfig,
        cancel: CancellationToken,
    ) -> Result<Self, NodeAddressServiceError> {
        Ok(Self {
            rpc_client,
            config,
            slot_receiver,
            leaders_sender,
            cancel,
        })
    }

    pub async fn run(self) -> Result<(), NodeAddressServiceError> {
        let Self {
            rpc_client,
            config,
            mut slot_receiver,
            leaders_sender,
            cancel,
        } = self;
        let lookahead_slots =
            (config.lookahead_leaders as u64).saturating_mul(NUM_CONSECUTIVE_LEADER_SLOTS);

        let mut leader_tpu_cache = LeaderTpuCacheServiceState::new(0, 0, 0, vec![], vec![]);

        let mut num_consequent_failures: usize = 0;
        let mut last_cluster_refresh = Instant::now() - config.refresh_every; // to ensure we refresh immediately
        loop {
            tokio::select! {
                res = slot_receiver.changed() => {
                    if let Err(e) = res {
                        warn!("Slot receiver channel closed: {e}");
                        break;
                    }

                    debug!("Slot update received, refreshing leader cache: {:?}", last_cluster_refresh.elapsed());
                    if let Err(e) = leader_tpu_cache.update(
                        &mut last_cluster_refresh,
                        config.refresh_every,
                        &rpc_client,
                        &slot_receiver
                    ).await {
                        debug!("Failed to update leader cache: {e}");
                        num_consequent_failures = num_consequent_failures.saturating_add(1);
                        if num_consequent_failures >= config.max_consecutive_failures {
                            error!("Failed to update leader cache {} times, giving up", num_consequent_failures);
                            return Err(e);
                        }
                    } else {
                        debug!("Leader cache updated successfully");
                        num_consequent_failures = 0;
                    }

                    let current_slot = slot_receiver.borrow().0;
                    let leaders = leader_tpu_cache.get_leader_sockets(
                        current_slot, lookahead_slots);
                    if let Err(e) = leaders_sender.send((current_slot, leaders)) {
                        warn!("Unexpectly dropped leaders_sender: {e}");
                        break;
                    }
                }

                _ = cancel.cancelled() => {
                    info!("Cancel signal received, stopping LeaderTpuCacheService.");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[derive(PartialEq, Debug)]
struct LeaderTpuCacheServiceState {
    first_slot: Slot,
    leaders: Vec<Pubkey>,
    leader_tpu_map: HashMap<Pubkey, SocketAddr>,
    slots_in_epoch: Slot,
    last_slot_in_epoch: Slot,
}

impl LeaderTpuCacheServiceState {
    pub fn new(
        first_slot: Slot,
        slots_in_epoch: Slot,
        last_slot_in_epoch: Slot,
        leaders: Vec<Pubkey>,
        cluster_nodes: Vec<RpcContactInfo>,
    ) -> Self {
        let leader_tpu_map = Self::extract_cluster_tpu_sockets(cluster_nodes);
        Self {
            first_slot,
            leaders,
            leader_tpu_map,
            slots_in_epoch,
            last_slot_in_epoch,
        }
    }

    // Last slot that has a cached leader pubkey
    pub fn last_slot(&self) -> Slot {
        self.first_slot + self.leaders.len().saturating_sub(1) as u64
    }

    pub fn slot_info(&self) -> (Slot, Slot, Slot) {
        (
            self.last_slot(),
            self.last_slot_in_epoch,
            self.slots_in_epoch,
        )
    }

    // Get the TPU sockets for the current leader and upcoming leaders according
    // to fanout size.
    fn get_leader_sockets(
        &self,
        estimated_current_slot: Slot,
        fanout_slots: u64,
    ) -> Vec<SocketAddr> {
        let mut leader_sockets = Vec::with_capacity(
            ((fanout_slots + NUM_CONSECUTIVE_LEADER_SLOTS - 1) / NUM_CONSECUTIVE_LEADER_SLOTS)
                as usize,
        );
        // `first_slot` might have been advanced since caller last read the
        // `estimated_current_slot` value. Take the greater of the two values to
        // ensure we are reading from the latest leader schedule.
        let current_slot = std::cmp::max(estimated_current_slot, self.first_slot);
        for leader_slot in (current_slot..current_slot + fanout_slots)
            .step_by(NUM_CONSECUTIVE_LEADER_SLOTS as usize)
        {
            if let Some(leader) = self.get_slot_leader(leader_slot) {
                if let Some(tpu_socket) = self.leader_tpu_map.get(leader) {
                    leader_sockets.push(*tpu_socket);
                    debug!("Pushed leader {leader} TPU socket: {tpu_socket}");
                } else {
                    // The leader is probably delinquent
                    debug!("TPU not available for leader {leader}");
                }
            } else {
                // Overran the local leader schedule cache
                warn!(
                    "Leader not known for slot {}; cache holds slots [{},{}]",
                    leader_slot,
                    self.first_slot,
                    self.last_slot()
                );
            }
        }
        leader_sockets
    }

    pub fn get_slot_leader(&self, slot: Slot) -> Option<&Pubkey> {
        if slot >= self.first_slot {
            let index = slot - self.first_slot;
            self.leaders.get(index as usize)
        } else {
            None
        }
    }

    pub fn fanout(slots_in_epoch: Slot) -> Slot {
        (2 * MAX_FANOUT_SLOTS).min(slots_in_epoch)
    }

    fn extract_cluster_tpu_sockets(
        cluster_contact_info: Vec<RpcContactInfo>,
    ) -> HashMap<Pubkey, SocketAddr> {
        cluster_contact_info
            .into_iter()
            .filter_map(|contact_info| {
                let pubkey = Pubkey::from_str(&contact_info.pubkey).ok()?;
                let socket = {
                    contact_info.tpu_quic.or_else(|| {
                        let mut socket = contact_info.tpu?;
                        let port = socket.port().checked_add(QUIC_PORT_OFFSET)?;
                        socket.set_port(port);
                        Some(socket)
                    })
                }?;
                Some((pubkey, socket))
            })
            .collect()
    }

    /// Update the leader cache with the latest slot leaders and cluster TPU ports.
    async fn update(
        &mut self,
        last_cluster_refresh: &mut Instant,
        refresh_nodes_every: Duration,
        rpc_client: &RpcClient,
        slot_receiver: &watch::Receiver<(Slot, u64, Duration)>,
    ) -> Result<(), NodeAddressServiceError> {
        // even if some intermediate step fails, we still want to update state
        // at least partially.
        let mut result = Ok(());
        // Refresh cluster TPU ports every `refresh_every` in case validators restart with
        // new port configuration or new validators come online
        debug!(
            "Refreshing cluster TPU sockets every {:?}, {:?}",
            last_cluster_refresh.elapsed(),
            refresh_nodes_every
        );
        if last_cluster_refresh.elapsed() > refresh_nodes_every {
            let cluster_nodes = rpc_client.get_cluster_nodes().await;
            match cluster_nodes {
                Ok(cluster_nodes) => {
                    self.leader_tpu_map = Self::extract_cluster_tpu_sockets(cluster_nodes);
                    *last_cluster_refresh = Instant::now();
                }
                Err(err) => {
                    warn!("Failed to fetch cluster tpu sockets: {err}");
                    result = Err(NodeAddressServiceError::RpcError(err.into()));
                }
            }
        }

        let (mut estimated_current_slot, start_time, estimated_slot_duration) =
            *slot_receiver.borrow();

        // If we are close to the end of slot, increment the estimated current slot
        if timestamp() - start_time >= estimated_slot_duration.as_millis() as u64 {
            estimated_current_slot = estimated_current_slot.saturating_add(1);
        }

        let (last_slot, last_slot_in_epoch, slots_in_epoch) = self.slot_info();

        debug!(
            "Estimated current slot: {}, last slot: {}, last slot in epoch: {}, slots in epoch: {}",
            estimated_current_slot, last_slot, last_slot_in_epoch, slots_in_epoch
        );
        // If we're crossing into a new epoch, fetch the updated epoch schedule.
        if estimated_current_slot > last_slot_in_epoch {
            debug!(
                "Crossing into a new epoch, fetching updated epoch schedule. \
                 Last slot in epoch: {}, estimated current slot: {}",
                last_slot_in_epoch, estimated_current_slot
            );
            match rpc_client.get_epoch_schedule().await {
                Ok(epoch_schedule) => {
                    let epoch = epoch_schedule.get_epoch(estimated_current_slot);
                    self.slots_in_epoch = epoch_schedule.get_slots_in_epoch(epoch);
                    self.last_slot_in_epoch = epoch_schedule.get_last_slot_in_epoch(epoch);
                }
                Err(err) => {
                    warn!("Failed to fetch epoch schedule: {err}");
                    result = Err(NodeAddressServiceError::RpcError(err.into()));
                }
            }
        }

        // If we are within the fanout range of the last slot in the cache,
        // fetch more slot leaders. We pull down a big batch at at time to
        // amortize the cost of the RPC call. We don't want to stall
        // transactions on pulling this down so we fetch it proactively.
        if estimated_current_slot >= last_slot.saturating_sub(MAX_FANOUT_SLOTS) {
            let slot_leaders = rpc_client
                .get_slot_leaders(
                    estimated_current_slot,
                    LeaderTpuCacheServiceState::fanout(slots_in_epoch),
                )
                .await;
            match slot_leaders {
                Ok(slot_leaders) => {
                    self.first_slot = estimated_current_slot;
                    self.leaders = slot_leaders;
                }
                Err(err) => {
                    warn!(
                        "Failed to fetch slot leaders (first_slot: \
                         {}): {err}",
                        estimated_current_slot
                    );
                    result = Err(NodeAddressServiceError::RpcError(err.into()));
                }
            }
        }
        result
    }
}

// 48 chosen because it's unlikely that 12 leaders in a row will miss their slots
const MAX_SLOT_SKIP_DISTANCE: u64 = 48;

#[derive(Debug)]
pub(crate) struct RecentLeaderSlots(VecDeque<Slot>);
impl RecentLeaderSlots {
    pub(crate) fn new(current_slot: Slot) -> Self {
        let mut recent_slots = VecDeque::new();
        recent_slots.push_back(current_slot);
        Self(recent_slots)
    }

    pub(crate) fn record_slot(&mut self, current_slot: Slot) {
        self.0.push_back(current_slot);
        // 12 recent slots should be large enough to avoid a misbehaving
        // validator from affecting the median recent slot
        while self.0.len() > 12 {
            self.0.pop_front();
        }
    }

    // Estimate the current slot from recent slot notifications.
    pub(crate) fn estimate_current_slot(&self) -> Slot {
        let mut recent_slots: Vec<Slot> = self.0.iter().cloned().collect();
        assert!(!recent_slots.is_empty());
        recent_slots.sort_unstable();
        debug!("Recent slots: {:?}", recent_slots);

        // Validators can broadcast invalid blocks that are far in the future
        // so check if the current slot is in line with the recent progression.
        let max_index = recent_slots.len() - 1;
        let median_index = max_index / 2;
        let median_recent_slot = recent_slots[median_index];
        let expected_current_slot = median_recent_slot + (max_index - median_index) as u64;
        let max_reasonable_current_slot = expected_current_slot + MAX_SLOT_SKIP_DISTANCE;

        // Return the highest slot that doesn't exceed what we believe is a
        // reasonable slot.
        let res = recent_slots
            .into_iter()
            .rev()
            .find(|slot| *slot <= max_reasonable_current_slot)
            .unwrap();
        debug!(
            "expected_current_slot: {}, max reasonable current slot: {}, found: {}",
            expected_current_slot, max_reasonable_current_slot, res
        );

        return res;
    }
}

#[cfg(test)]
impl From<Vec<Slot>> for RecentLeaderSlots {
    fn from(recent_slots: Vec<Slot>) -> Self {
        assert!(!recent_slots.is_empty());
        Self(recent_slots.into_iter().collect())
    }
}
