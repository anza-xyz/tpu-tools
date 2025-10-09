use {
    crate::{
        run_rate_latency_tool_scheduler::LeaderSlotEstimator,
        yellowstone_subscriber::{create_client_config, create_geyser_client},
    },
    futures_util::stream::StreamExt,
    log::*,
    node_address_service::{
        leader_tpu_cache_service::LeaderUpdateReceiver,
        websocket_slot_update_service::{RecentLeaderSlots, SlotEstimator},
        LeaderTpuCacheService, LeaderTpuCacheServiceConfig, NodeAddressServiceError, SlotReceiver,
    },
    solana_clock::{Slot, DEFAULT_MS_PER_SLOT},
    solana_commitment_config::CommitmentConfig,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_time_utils::timestamp,
    solana_tpu_client_next::leader_updater::LeaderUpdater,
    std::{collections::HashMap, net::SocketAddr, sync::Arc},
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

pub struct YellowstoneNodeAddressService {
    leaders_receiver: LeaderUpdateReceiver,
    slot_receiver: SlotReceiver,
    slot_update_service: YellowstoneSlotUpdateService,
    leader_cache_service: LeaderTpuCacheService,
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

        let (slot_receiver, slot_update_service) =
            YellowstoneSlotUpdateService::run(start_slot, yellowstone_url, cancel.clone()).await?;
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
impl LeaderUpdater for YellowstoneNodeAddressService {
    //TODO(klykov): we need to consier removing next leaders with lookahead_leaders in the future
    fn next_leaders(&mut self, _lookahead_leaders: usize) -> Vec<SocketAddr> {
        self.leaders_receiver.leaders()
    }

    //TODO(klykov): stop should return error to handle join and also stop should
    //consume the object. We cannot properly implement it because it will break
    //API.
    async fn stop(&mut self) {}
}

#[async_trait]
impl LeaderSlotEstimator for YellowstoneNodeAddressService {
    fn get_current_slot(&mut self) -> Slot {
        self.slot_receiver.slot_with_timestamp().0
    }
}

pub struct YellowstoneSlotUpdateService {
    handle: JoinHandle<Result<(), NodeAddressServiceError>>,
    cancel: CancellationToken,
}

impl YellowstoneSlotUpdateService {
    pub async fn run(
        current_slot: Slot,
        yellowstone_url: String,
        cancel: CancellationToken,
    ) -> Result<(SlotReceiver, Self), NodeAddressServiceError> {
        assert!(
            !yellowstone_url.is_empty(),
            "Yellowstone URL must not be empty"
        );
        let client_config = create_client_config(&yellowstone_url);
        let mut client = create_geyser_client(client_config).await.map_err(|e| {
            error!("Failed to create Yellowstone client: {e:?}");
            NodeAddressServiceError::InitializationFailed
        })?;

        let request = Self::build_request();

        let mut recent_slots = RecentLeaderSlots::new();
        let (slot_sender, slot_receiver) = watch::channel((current_slot, timestamp()));
        let slot_receiver_clone = slot_receiver.clone();
        let cancel_clone = cancel.clone();

        // Track the last time a slot update was received. In case of current leader is not sending relevant shreds for some reason, the current slot will not update.
        let mut last_slot_time = Instant::now();
        const FALLBACK_SLOT_TIMEOUT: Duration = Duration::from_millis(DEFAULT_MS_PER_SLOT);

        let mut interval = interval(FALLBACK_SLOT_TIMEOUT);
        let (_subscribe_tx, mut stream) = client
            .subscribe_with_request(Some(request))
            .await
            .map_err(|e| {
                error!("Failed to subscribe to Yellowstone: {e:?}");
                NodeAddressServiceError::InitializationFailed
            })?;

        let main_loop = async move {
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
                                    slot_sender.send((estimated_slot, timestamp() ))
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

    pub async fn shutdown(self) -> Result<(), NodeAddressServiceError> {
        self.cancel.cancel();
        self.handle.await??;
        Ok(())
    }
}
