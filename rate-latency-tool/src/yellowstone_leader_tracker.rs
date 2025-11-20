use {
    crate::{
        run_rate_latency_tool_scheduler::LeaderSlotEstimator,
        yellowstone_subscriber::{create_client_config, create_geyser_client, YellowstoneError},
    },
    futures::Stream,
    futures_util::stream::StreamExt,
    log::*,
    solana_clock::Slot,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_tpu_client_next::{
        leader_updater::LeaderUpdater,
        node_address_service::{
            LeaderTpuCacheServiceConfig, NodeAddressService, NodeAddressServiceError, SlotEvent,
        },
    },
    std::{collections::HashMap, net::SocketAddr, sync::Arc},
    thiserror::Error,
    tokio_util::sync::CancellationToken,
    tonic::{async_trait, Status},
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel, SlotStatus, SubscribeRequest,
        SubscribeRequestFilterSlots, SubscribeUpdate, SubscribeUpdateSlot,
    },
};

pub struct YellowstoneNodeAddressService {
    service: NodeAddressService,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    YellowstoneError(#[from] YellowstoneError),

    #[error(transparent)]
    NodeAddressServiceError(#[from] NodeAddressServiceError),
}

impl YellowstoneNodeAddressService {
    pub async fn run(
        rpc_client: Arc<RpcClient>,
        yellowstone_url: String,
        yellowstone_token: Option<&str>,
        config: LeaderTpuCacheServiceConfig,
        cancel: CancellationToken,
    ) -> Result<Self, Error> {
        let stream = init_stream(yellowstone_url.clone(), yellowstone_token).await?;
        let filtered_stream =
            stream.filter_map(|update| async { map_yellowstone_update_to_slot_event(update) });

        let service = NodeAddressService::run(rpc_client, filtered_stream, config, cancel).await?;

        Ok(Self { service })
    }

    pub async fn shutdown(&mut self) -> Result<(), Error> {
        self.service.shutdown().await?;
        Ok(())
    }
}

async fn init_stream(
    yellowstone_url: String,
    yellowstone_token: Option<&str>,
) -> Result<impl Stream<Item = Result<SubscribeUpdate, Status>> + Send + 'static, Error> {
    assert!(
        !yellowstone_url.is_empty(),
        "Yellowstone URL must not be empty"
    );
    let client_config = create_client_config(&yellowstone_url, yellowstone_token);
    let mut client = create_geyser_client(client_config).await.map_err(|e| {
        error!("Failed to create Yellowstone client: {e:?}");
        e
    })?;

    let request = build_request();

    let (_subscribe_tx, stream) =
        client
            .subscribe_with_request(Some(request))
            .await
            .map_err(|e| {
                error!("Failed to subscribe to Yellowstone: {e:?}");
                YellowstoneError::GeyserGrpcClientError(e)
            })?;
    Ok(stream)
}

fn map_yellowstone_update_to_slot_event(
    update: Result<SubscribeUpdate, Status>,
) -> Option<SlotEvent> {
    if update.is_err() {
        error!("Error received from Yellowstone: {:?}", update.err());
        return None;
    }
    match update
        .unwrap()
        .update_oneof
        .expect("Should be valid message")
    {
        UpdateOneof::Slot(SubscribeUpdateSlot { slot, status, .. }) => {
            match SlotStatus::try_from(status).expect("Should be valid status code") {
                // This update indicates that we have just received the first shred from
                // the leader for this slot and they are probably still accepting transactions.
                SlotStatus::SlotFirstShredReceived => Some(SlotEvent::Start(slot)),
                //TODO(klykov): fall back on bank created to use with solana test validator
                // This update indicates that a full slot was received by the connected
                // node so we can stop sending transactions to the leader for that slot
                SlotStatus::SlotCompleted => Some(SlotEvent::End(slot)),
                _ => None,
            }
        }
        _ => {
            error!("Unexpected update type received from Yellowstone");
            None
        }
    }
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

#[async_trait]
impl LeaderUpdater for YellowstoneNodeAddressService {
    fn next_leaders(&mut self, lookahead_leaders: usize) -> Vec<SocketAddr> {
        self.service.next_leaders(lookahead_leaders)
    }

    async fn stop(&mut self) {
        let _ = self.shutdown().await;
    }
}

#[async_trait]
impl LeaderSlotEstimator for YellowstoneNodeAddressService {
    fn get_current_slot(&mut self) -> Slot {
        self.service.estimated_current_slot()
    }
}
