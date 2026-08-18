use {
    crate::{
        backpressured_broadcaster::BackpressuredBroadcaster,
        cli::{EndpointConfig, ExecutionParams, TransactionParams},
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
        accounts_file::AccountsFile, blockhash_updater::BlockhashUpdater,
        leader_updater::create_leader_updater,
    },
    std::{
        fmt::Debug,
        num::{NonZeroU64, NonZeroUsize},
        sync::Arc,
        time::Duration,
    },
    tokio::{
        sync::{mpsc, oneshot, watch},
        task::JoinHandle,
    },
    tokio_util::sync::CancellationToken,
};

const GENERATOR_CHANNEL_SIZE: usize = 32;

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

pub async fn run_client(
    rpc_client: Arc<RpcClient>,
    websocket_url: String,
    accounts: AccountsFile,
    transaction_params: TransactionParams,
    execution_params: ExecutionParams,
    stats_sender: Option<RunClientStatsSender>,
    cancel: CancellationToken,
) -> Result<(), BenchClientError> {
    let endpoint_configs = execution_params
        .resolved_endpoint_configs()
        .map_err(BenchClientError::InvalidCliArguments)?;
    let endpoint_identities = endpoint_configs
        .iter()
        .map(load_identity)
        .collect::<Result<Vec<_>, _>>()?;

    let generate_tx_batch_size = transaction_params
        .simple_transfer_tx_params
        .tx_batch_size
        .get();
    let workers_pull_size = execution_params.workers_pull_size.get();
    let num_transactions = execution_params.num_transactions.map(NonZeroU64::get);
    let target_tps = execution_params.target_tps.map(NonZeroU64::get);
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

    let priority_fee_mode = PriorityFeeMode::try_from(&execution_params.priority_fee_params)
        .map_err(BenchClientError::InvalidCliArguments)?;
    let priority_fee_stats = Arc::new(PriorityFeeStats::default());

    let num_endpoints = endpoint_configs.len();
    let mut transaction_senders = Vec::with_capacity(num_endpoints);
    let mut transaction_receivers = Vec::with_capacity(num_endpoints);
    for _ in 0..num_endpoints {
        let (sender, receiver) = mpsc::channel(GENERATOR_CHANNEL_SIZE);
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
        execution_params.compute_unit_price,
        priority_fee_mode,
        priority_fee_stats.clone(),
        execution_params.duration,
        num_transactions,
        target_tps,
        generate_tx_batch_size,
        workers_pull_size,
    );

    let leader_updater_config = LeaderTpuCacheServiceConfig {
        lookahead_leaders: 4,
        refresh_nodes_info_every: Duration::from_secs(30),
        max_consecutive_failures: 5,
    };
    let (scheduler_handles, all_stats) = build_schedulers(
        rpc_client.clone(),
        websocket_url,
        &execution_params,
        endpoint_configs,
        endpoint_identities,
        transaction_receivers,
        leader_updater_config,
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

    join_service(transaction_generator_task_handle, "TransactionGenerator").await?;

    // The generator has dropped its senders. Keep our retained clones alive for
    // the drain window so the schedulers don't tear down while tpu-client-next's
    // worker queues and quinn send buffers still hold in-flight transactions.
    if execution_params.drain_seconds > 0 {
        info!(
            "Generator finished; draining in-flight transactions for {}s.",
            execution_params.drain_seconds
        );
        tokio::time::sleep(Duration::from_secs(execution_params.drain_seconds)).await;
    }
    // Closing the channels lets the tpu-client-next instances shut down.
    drop(drain_senders);

    join_service(blockhash_task_handle, "BlockhashUpdater").await?;
    for (i, handle) in scheduler_handles.into_iter().enumerate() {
        let name = format!("Scheduler-{i}");
        join_service(handle, &name).await?;
    }
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

async fn build_schedulers(
    rpc_client: Arc<RpcClient>,
    websocket_url: String,
    execution_params: &ExecutionParams,
    endpoint_configs: Vec<EndpointConfig>,
    endpoint_identities: Vec<Option<Keypair>>,
    transaction_receivers: Vec<mpsc::Receiver<WireTransaction>>,
    leader_updater_config: LeaderTpuCacheServiceConfig,
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
        let leader_updater = create_leader_updater(
            rpc_client.clone(),
            execution_params.leader_tracker.clone(),
            leader_updater_config.clone(),
            websocket_url.clone(),
            cancel.child_token(),
        )
        .await?;

        let stake_identity = validator_identity.as_ref().map(StakeIdentity::new);
        let scheduler_config = ConnectionWorkersSchedulerConfig {
            bind: BindTarget::Address(endpoint_config.bind),
            stake_identity,
            num_connections: NonZeroUsize::new(execution_params.num_max_open_connections)
                .expect("num-max-open-connections must be non-zero"),
            worker_channel_size: WORKER_CHANNEL_SIZE,
            max_reconnect_attempts: MAX_RECONNECT_ATTEMPTS,
            leaders_fanout: Fanout {
                send: execution_params.send_fanout,
                connect: execution_params.send_fanout.saturating_add(1),
            },
            override_initial_congestion_window: execution_params
                .initial_congestion_window
                .map(NonZeroU64::get),
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
