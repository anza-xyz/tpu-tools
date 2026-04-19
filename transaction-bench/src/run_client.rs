use {
    crate::{
        backpressured_broadcaster::BackpressuredBroadcaster,
        cli::{ExecutionParams, TransactionParams},
        error::BenchClientError,
        generator::TransactionGenerator,
    },
    log::*,
    solana_metrics::datapoint_info,
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_quic_definitions::{
        QUIC_MAX_STAKED_CONCURRENT_STREAMS, QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS,
        QUIC_MIN_STAKED_CONCURRENT_STREAMS, QUIC_TOTAL_STAKED_CONCURRENT_STREAMS,
    },
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_signer::{EncodableKey, Signer},
    solana_streamer::nonblocking::quic::ConnectionPeerType,
    solana_tpu_client_next::{
        ConnectionWorkersScheduler, SendTransactionStats,
        connection_workers_scheduler::{
            BindTarget, ConnectionWorkersSchedulerConfig, Fanout, StakeIdentity,
        },
        node_address_service::LeaderTpuCacheServiceConfig,
    },
    std::{fmt::Debug, num::NonZeroU64, sync::Arc, time::Duration},
    tokio::{
        sync::{mpsc, watch},
        task::JoinHandle,
    },
    tokio_util::sync::CancellationToken,
    tools_common::{
        accounts_file::AccountsFile, blockhash_updater::BlockhashUpdater,
        leader_updater::create_leader_updater,
    },
};

const GENERATOR_CHANNEL_SIZE: usize = 32;

/// Empirically chosen size of the connection worker channel. Lower/higher values gives
/// significantly smaller txs blocks on testnet.
const WORKER_CHANNEL_SIZE: usize = 20;
/// Number of reconnection attempts, a reasonable value that have been chosen,
/// doesn't affect TPS.
const MAX_RECONNECT_ATTEMPTS: usize = 5;

/// How often tpu-client-next reports network metrics.
const METRICS_REPORTING_INTERVAL: Duration = Duration::from_secs(1);

/// Default number of streams per connection if stake-based computation fails.
/// This failure happens if we use stake overrides.
const DEFAULT_NUM_STREAMS_PER_CONNECTION: usize = 8;
const TARGET_BATCHES_PER_SECOND: u64 = 10;

async fn find_node_activated_stake(
    rpc_client: &Arc<RpcClient>,
    node_id: Option<Pubkey>,
) -> Result<(Option<u64>, u64), BenchClientError> {
    let vote_accounts = rpc_client
        .get_vote_accounts()
        .await
        .map_err(|_| BenchClientError::FindValidatorIdentityFailure)?;

    let total_active_stake: u64 = vote_accounts
        .current
        .iter()
        .map(|vote_account| vote_account.activated_stake)
        .sum();

    let Some(node_id) = node_id else {
        return Ok((None, total_active_stake));
    };
    let node_id_as_str = node_id.to_string();
    let find_result = vote_accounts
        .current
        .iter()
        .find(|&vote_account| vote_account.node_pubkey == node_id_as_str);
    match find_result {
        Some(value) => Ok((Some(value.activated_stake), total_active_stake)),
        None => Err(BenchClientError::FindValidatorIdentityFailure),
    }
}

async fn compute_num_streams(
    rpc_client: &Arc<RpcClient>,
    validator_pubkey: Option<Pubkey>,
) -> Result<usize, BenchClientError> {
    let (validator_stake, total_stake) =
        find_node_activated_stake(rpc_client, validator_pubkey).await?;
    debug!(
        "Validator {validator_pubkey:?} stake: {validator_stake:?}, total stake: {total_stake}."
    );
    let client_type = validator_stake.map_or(ConnectionPeerType::Unstaked, |stake| {
        ConnectionPeerType::Staked(stake)
    });
    Ok(compute_max_allowed_uni_streams(client_type, total_stake))
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

/// Periodically reads and resets stats from all scheduler instances, sums them,
/// and reports the aggregate to InfluxDB under a single metric name.
#[allow(clippy::arithmetic_side_effects)]
async fn report_aggregated_stats(
    all_stats: Vec<Arc<SendTransactionStats>>,
    reporting_interval: Duration,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(reporting_interval);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let (mut connect_error, mut connection_error, mut successfully_sent,
                     mut congestion_events, mut write_error) = (0i64, 0i64, 0i64, 0i64, 0i64);
                for stats in &all_stats {
                    let view = stats.read_and_reset();
                    connect_error += (view.connect_error_cids_exhausted
                        + view.connect_error_other
                        + view.connect_error_invalid_remote_address) as i64;
                    connection_error += (view.connection_error_reset
                        + view.connection_error_cids_exhausted
                        + view.connection_error_timed_out
                        + view.connection_error_application_closed
                        + view.connection_error_transport_error
                        + view.connection_error_version_mismatch
                        + view.connection_error_locally_closed) as i64;
                    successfully_sent += view.successfully_sent as i64;
                    congestion_events += view.transport_congestion_events as i64;
                    write_error += (view.write_error_stopped
                        + view.write_error_closed_stream
                        + view.write_error_connection_lost
                        + view.write_error_zero_rtt_rejected) as i64;
                }
                datapoint_info!(
                    "transaction-bench-network",
                    ("connect_error", connect_error, i64),
                    ("connection_error", connection_error, i64),
                    ("successfully_sent", successfully_sent, i64),
                    ("congestion_events", congestion_events, i64),
                    ("write_error", write_error, i64),
                );
            }
            _ = cancel.cancelled() => break,
        }
    }
}

pub async fn run_client(
    rpc_client: Arc<RpcClient>,
    websocket_url: String,
    accounts: AccountsFile,
    transaction_params: TransactionParams,
    ExecutionParams {
        staked_identity_files,
        bind,
        duration,
        target_tps,
        num_max_open_connections,
        workers_pull_size,
        send_fanout,
        //TODO(klykov): pass to tx generator
        compute_unit_price: _,
        leader_tracker,
    }: ExecutionParams,
) -> Result<(), BenchClientError> {
    let validator_identities: Vec<Keypair> = staked_identity_files
        .into_iter()
        .map(|path| {
            Keypair::read_from_file(&path).map_err(|_err| BenchClientError::KeypairReadFailure)
        })
        .collect::<Result<_, _>>()?;
    let num_tpu_clients = validator_identities.len().max(1);

    // Set up size of the txs batch to put into the queue to be equal to the num_streams_per_connection
    let num_streams_per_connection = compute_num_streams(
        &rpc_client,
        validator_identities.first().map(|keypair| keypair.pubkey()),
    )
    .await
    .unwrap_or(DEFAULT_NUM_STREAMS_PER_CONNECTION);
    let tx_batch_size = transaction_params
        .simple_transfer_tx_params
        .tx_batch_size
        .map(|n| n.get());
    let send_batch_size =
        compute_send_batch_size(tx_batch_size, num_streams_per_connection, target_tps);
    let workers_pull_size =
        compute_workers_pull_size(workers_pull_size, send_batch_size, target_tps);
    info!("Number of streams per connection is {num_streams_per_connection}.");
    if let Some(tx_batch_size) = tx_batch_size {
        info!("Using tx batch size override: {tx_batch_size}.");
    } else if let Some(target_tps) = target_tps {
        info!("Using rate-limited tx batch size {send_batch_size} for target {target_tps} tx/s.");
    }
    if let Some(target_tps) = target_tps {
        info!("Using {workers_pull_size} generator workers for target {target_tps} tx/s.");
    }

    if let Some(num_conflict_groups) = transaction_params
        .simple_transfer_tx_params
        .num_conflict_groups
    {
        let num_send_instructions_per_tx = transaction_params
            .simple_transfer_tx_params
            .num_send_instructions_per_tx;
        let max_groups = num_send_instructions_per_tx.saturating_mul(send_batch_size);
        let num_conflict_groups = num_conflict_groups.get();

        if num_conflict_groups > max_groups {
            return Err(BenchClientError::InvalidCliArguments(format!(
                "--num-conflict-groups ({num_conflict_groups}) must be <= \
                 num-send-instructions-per-tx ({num_send_instructions_per_tx}) * tx-batch-size \
                 ({send_batch_size})"
            )));
        }
    }

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

    // Create N channels, one per tpu-client-next instance.
    let mut transaction_senders = Vec::with_capacity(num_tpu_clients);
    let mut transaction_receivers = Vec::with_capacity(num_tpu_clients);
    for _ in 0..num_tpu_clients {
        let (sender, receiver) = mpsc::channel(GENERATOR_CHANNEL_SIZE);
        transaction_senders.push(sender);
        transaction_receivers.push(receiver);
    }

    let transaction_generator = TransactionGenerator::new(
        accounts,
        blockhash_receiver,
        transaction_senders,
        transaction_params,
        send_batch_size,
        duration,
        target_tps,
        workers_pull_size,
    );

    let cancel = CancellationToken::new();
    let transaction_generator_task_handle =
        tokio::spawn(async move { transaction_generator.run().await });
    let config = LeaderTpuCacheServiceConfig {
        lookahead_leaders: 4,
        refresh_nodes_info_every: Duration::from_secs(30),
        max_consecutive_failures: 5,
    };

    if num_tpu_clients > 1 {
        info!("Spawning {num_tpu_clients} tpu-client-next instances.");
    }

    let mut scheduler_handles: Vec<JoinHandle<Result<(), BenchClientError>>> =
        Vec::with_capacity(num_tpu_clients);
    let mut all_stats: Vec<Arc<SendTransactionStats>> = Vec::with_capacity(num_tpu_clients);
    for (i, transaction_receiver) in transaction_receivers.into_iter().enumerate() {
        let leader_updater = create_leader_updater(
            rpc_client.clone(),
            leader_tracker.clone(),
            config.clone(),
            websocket_url.clone(),
            cancel.clone(),
        )
        .await?;

        let stake_identity = validator_identities
            .get(i)
            .map(|ident| StakeIdentity::new(ident));
        let scheduler_config = ConnectionWorkersSchedulerConfig {
            bind: BindTarget::Address(bind),
            stake_identity,
            num_connections: num_max_open_connections,
            worker_channel_size: WORKER_CHANNEL_SIZE,
            max_reconnect_attempts: MAX_RECONNECT_ATTEMPTS,
            leaders_fanout: Fanout {
                send: send_fanout,
                connect: send_fanout.saturating_add(1),
            },
            skip_check_transaction_age: false,
            override_initial_congestion_window: None,
        };

        let (_, update_identity_receiver) = watch::channel(None);
        let cancel_clone = cancel.clone();
        let scheduler = ConnectionWorkersScheduler::new(
            leader_updater,
            transaction_receiver,
            update_identity_receiver,
            cancel_clone,
        );
        all_stats.push(scheduler.get_stats());

        let scheduler_handle: JoinHandle<Result<(), BenchClientError>> =
            tokio::spawn(async move {
                let broadcaster = Box::new(BackpressuredBroadcaster {});
                scheduler
                    .run_with_broadcaster(scheduler_config, broadcaster)
                    .await?;
                Ok(())
            });
        scheduler_handles.push(scheduler_handle);
    }

    // Single metrics reporter aggregating stats across all tpu-client-next instances.
    tokio::spawn(report_aggregated_stats(
        all_stats,
        METRICS_REPORTING_INTERVAL,
        cancel,
    ));

    join_service(transaction_generator_task_handle, "TransactionGenerator").await?;
    join_service(blockhash_task_handle, "BlockhashUpdater").await?;
    for (i, handle) in scheduler_handles.into_iter().enumerate() {
        let name = format!("Scheduler-{i}");
        join_service(handle, &name).await?;
    }
    Ok(())
}

#[allow(clippy::arithmetic_side_effects)]
fn compute_send_batch_size(
    tx_batch_size_override: Option<usize>,
    num_streams_per_connection: usize,
    target_tps: Option<NonZeroU64>,
) -> usize {
    tx_batch_size_override.unwrap_or_else(|| {
        target_tps.map_or(num_streams_per_connection, |target_tps| {
            let target_tps = target_tps.get();
            let target_batch_size = target_tps.div_ceil(TARGET_BATCHES_PER_SECOND);
            usize::try_from(target_batch_size)
                .unwrap_or(usize::MAX)
                .clamp(1, num_streams_per_connection)
        })
    })
}

#[allow(clippy::arithmetic_side_effects)]
fn compute_workers_pull_size(
    configured_workers_pull_size: usize,
    send_batch_size: usize,
    target_tps: Option<NonZeroU64>,
) -> usize {
    target_tps.map_or(configured_workers_pull_size, |target_tps| {
        let target_batches_per_sec = target_tps
            .get()
            .div_ceil(u64::try_from(send_batch_size).unwrap_or(u64::MAX));
        let guessed_workers = match target_batches_per_sec {
            0..=1 => 1,
            2..=10 => 2,
            _ => 4,
        };
        guessed_workers.min(configured_workers_pull_size.max(1))
    })
}

// Private function copied from streamer::nonblocking::swqos
#[allow(clippy::arithmetic_side_effects)]
fn compute_max_allowed_uni_streams(peer_type: ConnectionPeerType, total_stake: u64) -> usize {
    match peer_type {
        ConnectionPeerType::Staked(peer_stake) => {
            // No checked math for f64 type. So let's explicitly check for 0 here
            if total_stake == 0 || peer_stake > total_stake {
                warn!(
                    "Invalid stake values: peer_stake: {peer_stake:?}, total_stake: \
                     {total_stake:?}"
                );

                QUIC_MIN_STAKED_CONCURRENT_STREAMS
            } else {
                let delta = (QUIC_TOTAL_STAKED_CONCURRENT_STREAMS
                    - QUIC_MIN_STAKED_CONCURRENT_STREAMS) as f64;

                (((peer_stake as f64 / total_stake as f64) * delta) as usize
                    + QUIC_MIN_STAKED_CONCURRENT_STREAMS)
                    .clamp(
                        QUIC_MIN_STAKED_CONCURRENT_STREAMS,
                        QUIC_MAX_STAKED_CONCURRENT_STREAMS,
                    )
            }
        }
        ConnectionPeerType::Unstaked => QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS,
    }
}
