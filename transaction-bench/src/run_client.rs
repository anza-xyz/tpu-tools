use {
    crate::{
        backpressured_broadcaster::BackpressuredBroadcaster,
        cli::{EndpointConfig, PriorityFeeParams, TransactionParams},
        error::BenchClientError,
        generator::{TransactionGenerator, transaction_generator::check_num_conflict_groups},
        priority_fee::{PriorityFeeMode, PriorityFeeStats},
    },
    log::*,
    solana_keypair::Keypair,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_signer::EncodableKey,
    solana_tpu_client_next::{
        ConnectionWorkersScheduler, SendTransactionStats, WireTransaction,
        connection_workers_scheduler::{
            BindTarget, ConnectionWorkersSchedulerConfig, Fanout, StakeIdentity,
        },
        node_address_service::LeaderTpuCacheServiceConfig,
    },
    solana_tpu_tools_common::{
        accounts_file::AccountsFile,
        blockhash_updater::BlockhashUpdater,
        cli::LeaderTracker,
        leader_updater::{LeaderUpdaterFactory, create_leader_updater},
    },
    std::{fmt::Debug, num::NonZeroUsize, sync::Arc, time::Duration},
    tokio::{
        sync::{mpsc, oneshot, watch},
        task::JoinHandle,
    },
    tokio_util::sync::CancellationToken,
};

/// Empirically chosen size of the connection worker channel. Lower/higher values gives
/// significantly smaller txs blocks on testnet.
const WORKER_CHANNEL_SIZE: usize = 20;
/// Number of reconnection attempts, a reasonable value that have been chosen,
/// doesn't affect TPS.
const MAX_RECONNECT_ATTEMPTS: usize = 5;

pub struct RunClientStats {
    pub send_transaction_stats: Vec<Arc<SendTransactionStats>>,
    pub priority_fee_stats: Arc<PriorityFeeStats>,
}

pub type RunClientStatsSender = oneshot::Sender<RunClientStats>;

#[derive(Clone)]
pub struct ExecutionParams {
    pub endpoint_configs: Vec<EndpointConfig>,
    pub duration: Option<Duration>,
    pub num_transactions: Option<u64>,
    pub target_tps: Option<u64>,
    pub initial_congestion_window: Option<u64>,
    pub drain_seconds: u64,
    pub num_max_open_connections: usize,
    pub workers_pull_size: usize,
    pub send_fanout: usize,
    pub compute_unit_price: Option<u64>,
    pub priority_fee_params: PriorityFeeParams,
    pub leader_tracker: LeaderTracker,
}

pub async fn run_client(
    rpc_client: Arc<RpcClient>,
    websocket_url: String,
    accounts: AccountsFile,
    transaction_params: TransactionParams,
    ExecutionParams {
        endpoint_configs,
        duration,
        num_transactions,
        target_tps,
        initial_congestion_window,
        drain_seconds,
        num_max_open_connections,
        workers_pull_size,
        send_fanout,
        compute_unit_price,
        priority_fee_params,
        leader_tracker,
    }: ExecutionParams,
    stats_sender: Option<RunClientStatsSender>,
    cancel: CancellationToken,
) -> Result<(), BenchClientError> {
    let endpoint_identities = endpoint_configs
        .iter()
        .map(load_identity)
        .collect::<Result<Vec<_>, _>>()?;

    let generate_tx_batch_size = transaction_params
        .simple_transfer_tx_params
        .tx_batch_size
        .get();
    let generator_channel_size = workers_pull_size
        .checked_mul(generate_tx_batch_size)
        .ok_or_else(|| {
            BenchClientError::InvalidCliArguments(format!(
                "--workers-pull-size ({workers_pull_size}) * --tx-batch-size \
                 ({generate_tx_batch_size}) overflows usize"
            ))
        })?;
    if let Some(target_tps) = target_tps {
        info!("Using {workers_pull_size} generator workers for target {target_tps} tx/s.");
    }

    {
        let transfer_instructions_per_tx = transaction_params
            .simple_transfer_tx_params
            .num_send_instructions_per_tx;
        let transfer_instructions_per_batch =
            transfer_instructions_per_tx.saturating_mul(generate_tx_batch_size);
        let max_lamports_to_transfer = usize::try_from(
            transaction_params
                .simple_transfer_tx_params
                .max_lamports_to_transfer,
        )
        .unwrap_or(usize::MAX);
        if transfer_instructions_per_batch > max_lamports_to_transfer {
            return Err(BenchClientError::InvalidCliArguments(format!(
                "--max-lamports-to-transfer ({}) must be >= transfer instructions per generated \
                 batch ({}) computed as num-send-instructions-per-tx ({}) * tx-batch-size ({})",
                transaction_params
                    .simple_transfer_tx_params
                    .max_lamports_to_transfer,
                transfer_instructions_per_batch,
                transfer_instructions_per_tx,
                generate_tx_batch_size
            )));
        }
    }

    check_num_conflict_groups(
        transaction_params
            .simple_transfer_tx_params
            .num_conflict_groups,
        transaction_params
            .simple_transfer_tx_params
            .num_send_instructions_per_tx,
        generate_tx_batch_size,
    )?;

    if let Some(instruction_padding_config) = transaction_params.instruction_padding_config() {
        info!(
            "Checking for existence of instruction padding program: {}",
            instruction_padding_config.program_id
        );
        rpc_client
            .get_account(&instruction_padding_config.program_id)
            .await
            .map_err(|err| {
                BenchClientError::InvalidCliArguments(format!(
                    "instruction padding program {} is not available: {err}",
                    instruction_padding_config.program_id
                ))
            })?;
    }

    let blockhash = rpc_client
        .get_latest_blockhash()
        .await
        .expect("Blockhash request should not fail.");
    let (blockhash_sender, blockhash_receiver) = watch::channel(blockhash);
    let blockhash_updater = BlockhashUpdater::new(rpc_client.clone(), blockhash_sender);

    let blockhash_task_handle = tokio::spawn(async move { blockhash_updater.run().await });

    let priority_fee_mode = PriorityFeeMode::try_from(&priority_fee_params)
        .map_err(BenchClientError::InvalidCliArguments)?;
    let priority_fee_stats = Arc::new(PriorityFeeStats::default());

    let num_endpoints = endpoint_configs.len();
    let mut transaction_senders = Vec::with_capacity(num_endpoints);
    let mut transaction_receivers = Vec::with_capacity(num_endpoints);
    for _ in 0..num_endpoints {
        let (sender, receiver) = mpsc::channel(generator_channel_size);
        transaction_senders.push(sender);
        transaction_receivers.push(receiver);
    }

    // Retain a clone of the senders past the generator so the scheduler
    // channels stay open during the drain phase (see below); the generator
    // drops its own copies when it finishes.
    let drain_senders = transaction_senders.clone();

    let transaction_generator = TransactionGenerator::new(
        accounts,
        blockhash_receiver,
        transaction_senders,
        transaction_params,
        compute_unit_price,
        priority_fee_mode,
        priority_fee_stats.clone(),
        duration,
        num_transactions,
        target_tps,
        generate_tx_batch_size,
        workers_pull_size,
        cancel.child_token(),
    );

    let leader_updater_config = LeaderTpuCacheServiceConfig {
        lookahead_leaders: 4,
        refresh_nodes_info_every: Duration::from_secs(30),
        max_consecutive_failures: 5,
    };
    let leader_updater_factory = create_leader_updater(
        rpc_client.clone(),
        leader_tracker,
        leader_updater_config,
        websocket_url,
        cancel.child_token(),
    )
    .await?;
    let (scheduler_handles, all_stats) = build_schedulers(
        &leader_updater_factory,
        SchedulerConnectionParams {
            num_max_open_connections,
            send_fanout,
            initial_congestion_window,
        },
        endpoint_configs,
        endpoint_identities,
        transaction_receivers,
        cancel.clone(),
    )
    .await?;

    let transaction_generator_task_handle =
        tokio::spawn(async move { transaction_generator.run().await });
    if let Some(stats_sender) = stats_sender
        && stats_sender
            .send(RunClientStats {
                send_transaction_stats: all_stats,
                priority_fee_stats,
            })
            .is_err()
    {
        debug!("Stats receiver has been dropped.");
    }

    let mut result = join_service(transaction_generator_task_handle, "TransactionGenerator").await;
    if result.is_ok() {
        // The generator has dropped its senders. Keep our retained clones alive for
        // the drain window so the schedulers don't tear down while tpu-client-next's
        // worker queues and quinn send buffers still hold in-flight transactions.
        if drain_seconds > 0 {
            info!("Generator finished; draining in-flight transactions for {drain_seconds}s.");
            tokio::time::sleep(Duration::from_secs(drain_seconds)).await;
        }
    }
    // After the drain phase, cancel services before joining scheduler tasks. This
    // also wakes schedulers that are blocked below the transaction channel.
    cancel.cancel();
    // Closing the channels lets the tpu-client-next instances finish draining.
    drop(drain_senders);

    let blockhash_result = join_service(blockhash_task_handle, "BlockhashUpdater").await;
    if result.is_ok() {
        result = blockhash_result;
    }
    for (i, handle) in scheduler_handles.into_iter().enumerate() {
        let name = format!("Scheduler-{i}");
        let scheduler_result = join_service(handle, &name).await;
        if result.is_ok() {
            result = scheduler_result;
        }
    }
    let shutdown_result = leader_updater_factory.shutdown().await;
    result?;
    shutdown_result?;
    Ok(())
}

fn load_identity(endpoint_config: &EndpointConfig) -> Result<Option<Keypair>, BenchClientError> {
    endpoint_config
        .staked_identity_file
        .as_ref()
        .map(|staked_identity_file| {
            Keypair::read_from_file(staked_identity_file)
                .map_err(|_err| BenchClientError::KeypairReadFailure)
        })
        .transpose()
}

struct SchedulerConnectionParams {
    num_max_open_connections: usize,
    send_fanout: usize,
    initial_congestion_window: Option<u64>,
}

async fn build_schedulers(
    leader_updater_factory: &LeaderUpdaterFactory,
    scheduler_params: SchedulerConnectionParams,
    endpoint_configs: Vec<EndpointConfig>,
    endpoint_identities: Vec<Option<Keypair>>,
    transaction_receivers: Vec<mpsc::Receiver<WireTransaction>>,
    cancel: CancellationToken,
) -> Result<
    (
        Vec<JoinHandle<Result<(), BenchClientError>>>,
        Vec<Arc<SendTransactionStats>>,
    ),
    BenchClientError,
> {
    let mut scheduler_handles = Vec::with_capacity(endpoint_configs.len());
    let mut all_stats = Vec::with_capacity(endpoint_configs.len());

    for ((endpoint_config, validator_identity), transaction_receiver) in endpoint_configs
        .into_iter()
        .zip(endpoint_identities)
        .zip(transaction_receivers)
    {
        let leader_updater = leader_updater_factory.create_updater().await?;

        let stake_identity = validator_identity.as_ref().map(StakeIdentity::new);
        let scheduler_config = ConnectionWorkersSchedulerConfig {
            bind: BindTarget::Address(endpoint_config.bind),
            stake_identity,
            num_connections: NonZeroUsize::new(scheduler_params.num_max_open_connections)
                .expect("num-max-open-connections must be non-zero"),
            worker_channel_size: WORKER_CHANNEL_SIZE,
            max_reconnect_attempts: MAX_RECONNECT_ATTEMPTS,
            leaders_fanout: Fanout {
                send: scheduler_params.send_fanout,
                connect: scheduler_params.send_fanout.saturating_add(1),
            },
            override_initial_congestion_window: scheduler_params.initial_congestion_window,
        };

        let (_, update_identity_receiver) = watch::channel(None);
        let scheduler = ConnectionWorkersScheduler::new(
            leader_updater,
            transaction_receiver,
            update_identity_receiver,
            cancel.clone(),
        );
        all_stats.push(scheduler.get_stats());

        let scheduler_handle = tokio::spawn(async move {
            let broadcaster = Box::new(BackpressuredBroadcaster {});
            scheduler
                .run_with_broadcaster(scheduler_config, broadcaster)
                .await?;
            Ok(())
        });
        scheduler_handles.push(scheduler_handle);
    }

    Ok((scheduler_handles, all_stats))
}

async fn join_service<Error>(
    handle: JoinHandle<Result<(), Error>>,
    task_name: &str,
) -> Result<(), BenchClientError>
where
    Error: Debug + Into<BenchClientError>,
{
    match handle.await {
        Ok(Ok(_)) => {
            info!("Task {task_name} completed successfully");
            Ok(())
        }
        Ok(Err(e)) => {
            error!("Task failed with error: {e:?}");
            Err(e.into())
        }
        Err(e) => {
            error!("Task was cancelled or panicked: {e:?}");
            Err(BenchClientError::TaskJoinFailure {
                task_name: task_name.to_string(),
                reason: e.to_string(),
            })
        }
    }
}
