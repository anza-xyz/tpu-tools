use {
    crate::{
        backpressured_broadcaster::BackpressuredBroadcaster,
        cli::{ExecutionParams, TransactionParams},
        error::BenchClientError,
        generator::TransactionGenerator,
        priority_fee::{PriorityFeeMode, PriorityFeeStats},
    },
    log::*,
    solana_keypair::Keypair,
    solana_metrics::datapoint_info,
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
    solana_tpu_tools_common::{
        accounts_file::AccountsFile, blockhash_updater::BlockhashUpdater,
        leader_updater::create_leader_updater,
    },
    std::{
        fmt::Debug,
        num::NonZeroU64,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    },
    tokio::{
        sync::{mpsc, watch},
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
///
/// `successfully_sent_total` accumulates a non-resetting view of the
/// `successfully_sent` counter across all instances so the drain phase can
/// observe stability without fighting the per-tick reset.
#[allow(clippy::arithmetic_side_effects)]
async fn report_aggregated_stats(
    all_stats: Vec<Arc<SendTransactionStats>>,
    priority_fee_stats: Arc<PriorityFeeStats>,
    reporting_interval: Duration,
    successfully_sent_total: Arc<AtomicU64>,
    congestion_events_total: Arc<AtomicU64>,
    write_error_total: Arc<AtomicU64>,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(reporting_interval);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let (mut connect_error, mut connection_error, mut successfully_sent,
                     mut congestion_events, mut write_error) = (0i64, 0i64, 0i64, 0i64, 0i64);
                let (mut ce_reset, mut ce_cids, mut ce_timed_out, mut ce_app_closed,
                     mut ce_transport, mut ce_version, mut ce_locally_closed) =
                    (0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
                let (mut we_stopped, mut we_closed_stream, mut we_conn_lost,
                     mut we_zero_rtt) = (0u64, 0u64, 0u64, 0u64);
                for stats in &all_stats {
                    let view = stats.read_and_reset();
                    connect_error += (view.connect_error_cids_exhausted
                        + view.connect_error_other
                        + view.connect_error_invalid_remote_address) as i64;
                    ce_reset += view.connection_error_reset;
                    ce_cids += view.connection_error_cids_exhausted;
                    ce_timed_out += view.connection_error_timed_out;
                    ce_app_closed += view.connection_error_application_closed;
                    ce_transport += view.connection_error_transport_error;
                    ce_version += view.connection_error_version_mismatch;
                    ce_locally_closed += view.connection_error_locally_closed;
                    connection_error += (view.connection_error_reset
                        + view.connection_error_cids_exhausted
                        + view.connection_error_timed_out
                        + view.connection_error_application_closed
                        + view.connection_error_transport_error
                        + view.connection_error_version_mismatch
                        + view.connection_error_locally_closed) as i64;
                    successfully_sent += view.successfully_sent as i64;
                    congestion_events += view.transport_congestion_events as i64;
                    we_stopped += view.write_error_stopped;
                    we_closed_stream += view.write_error_closed_stream;
                    we_conn_lost += view.write_error_connection_lost;
                    we_zero_rtt += view.write_error_zero_rtt_rejected;
                    write_error += (view.write_error_stopped
                        + view.write_error_closed_stream
                        + view.write_error_connection_lost
                        + view.write_error_zero_rtt_rejected) as i64;
                }
                let sent_total = successfully_sent_total
                    .fetch_add(successfully_sent as u64, Ordering::Relaxed)
                    + successfully_sent as u64;
                let cong_total = congestion_events_total
                    .fetch_add(congestion_events as u64, Ordering::Relaxed)
                    + congestion_events as u64;
                let werr_total = write_error_total
                    .fetch_add(write_error as u64, Ordering::Relaxed)
                    + write_error as u64;
                info!(
                    "tx-bench stats (last {}ms): sent={successfully_sent} \
                     congestion_events={congestion_events} write_error={write_error} \
                     connect_error={connect_error} connection_error={connection_error} \
                     | totals: sent={sent_total} congestion_events={cong_total} \
                     write_error={werr_total}",
                    reporting_interval.as_millis(),
                );
                if connection_error > 0 {
                    info!(
                        "  connection_error breakdown: reset={ce_reset} \
                         timed_out={ce_timed_out} application_closed={ce_app_closed} \
                         transport_error={ce_transport} cids_exhausted={ce_cids} \
                         version_mismatch={ce_version} locally_closed={ce_locally_closed}",
                    );
                }
                if write_error > 0 {
                    info!(
                        "  write_error breakdown: stopped={we_stopped} \
                         closed_stream={we_closed_stream} connection_lost={we_conn_lost} \
                         zero_rtt_rejected={we_zero_rtt}",
                    );
                }
                datapoint_info!(
                    "transaction-bench-network",
                    ("connect_error", connect_error, i64),
                    ("connection_error", connection_error, i64),
                    ("successfully_sent", successfully_sent, i64),
                    ("congestion_events", congestion_events, i64),
                    ("write_error", write_error, i64),
                );

                let (total_priority_fees, priority_fee_tx_count) =
                    priority_fee_stats.read_and_reset();
                datapoint_info!(
                    "transaction-bench-priority-fees",
                    ("total_priority_fees", total_priority_fees, i64),
                    ("tx_count", priority_fee_tx_count, i64),
                );
            }
            _ = cancel.cancelled() => break,
        }
    }
}

/// After the generator finishes, give tpu-client-next's worker mpsc queues and
/// quinn send buffers a chance to flush before the schedulers tear themselves
/// down. Returns once `successfully_sent_total` is stable across two
/// consecutive reporter ticks, then sleeps a short fixed tail to let quinn
/// flush bytes already handed to it.
///
/// `drain_timeout` is the hard wall-clock cap — the tail is clamped to the
/// remaining budget so total time never exceeds it.
async fn drain_in_flight(
    successfully_sent_total: Arc<AtomicU64>,
    reporting_interval: Duration,
    drain_timeout: Duration,
    tail_after_stable: Duration,
) {
    let start = tokio::time::Instant::now();
    // Sleep two reporting intervals between checks so the reporter has at
    // least one tick to flush any outstanding successfully_sent counts.
    let stability_interval = reporting_interval.saturating_mul(2);
    let mut last_total = successfully_sent_total.load(Ordering::Relaxed);
    loop {
        let remaining = drain_timeout.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            warn!(
                "Drain phase hit the {drain_timeout:?} timeout; \
                 successfully_sent={last_total}. Tearing down anyway."
            );
            return;
        }
        let sleep_for = stability_interval.min(remaining);
        tokio::time::sleep(sleep_for).await;
        let now_total = successfully_sent_total.load(Ordering::Relaxed);
        if now_total == last_total {
            let remaining = drain_timeout.saturating_sub(start.elapsed());
            let tail = tail_after_stable.min(remaining);
            info!(
                "Drain phase reached stability at successfully_sent={last_total} after \
                 {elapsed:?}; sleeping tail {tail:?} (cap {drain_timeout:?}).",
                elapsed = start.elapsed(),
            );
            tokio::time::sleep(tail).await;
            return;
        }
        last_total = now_total;
    }
}

/// Fixed tail wait after stats stop growing. Lets quinn finish flushing bytes
/// that were already written to its send buffer.
const DRAIN_TAIL_AFTER_STABLE: Duration = Duration::from_secs(1);

pub async fn run_client(
    rpc_client: Arc<RpcClient>,
    websocket_url: String,
    accounts: AccountsFile,
    transaction_params: TransactionParams,
    ExecutionParams {
        staked_identity_files,
        bind,
        duration,
        num_transactions,
        target_tps,
        initial_congestion_window,
        drain_seconds,
        num_max_open_connections,
        clients_per_identity,
        workers_pull_size,
        send_fanout,
        compute_unit_price,
        priority_fee_params,
        leader_tracker,
    }: ExecutionParams,
) -> Result<(), BenchClientError> {
    let validator_identities: Vec<Keypair> = staked_identity_files
        .into_iter()
        .map(|path| {
            Keypair::read_from_file(&path).map_err(|_err| BenchClientError::KeypairReadFailure)
        })
        .collect::<Result<_, _>>()?;
    // Each tpu-client-next instance opens one connection per leader. To get
    // multiple connections under a single identity (without repeating
    // --staked-identity-file), spawn `clients_per_identity` instances per
    // identity. With no identity file, `num_identities` is 1 and these are
    // unstaked instances.
    let num_identities = validator_identities.len().max(1);
    let num_tpu_clients = num_identities.saturating_mul(clients_per_identity.max(1));

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

    // Extra sender clones kept alive past the generator so that the schedulers
    // don't see `None` (and start tearing down workers) until after the drain
    // phase has had a chance to flush in-flight queues.
    let drain_senders: Vec<mpsc::Sender<_>> = transaction_senders.clone();

    let priority_fee_mode = PriorityFeeMode::try_from(&priority_fee_params)
        .map_err(BenchClientError::InvalidCliArguments)?;
    let priority_fee_stats = Arc::new(PriorityFeeStats::default());
    let transaction_generator = TransactionGenerator::new(
        accounts,
        blockhash_receiver,
        transaction_senders,
        transaction_params,
        compute_unit_price,
        priority_fee_mode,
        priority_fee_stats.clone(),
        send_batch_size,
        duration,
        num_transactions,
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
            .get(i % num_identities)
            .map(StakeIdentity::new);
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
            override_initial_congestion_window: initial_congestion_window.map(NonZeroU64::get),
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

        let scheduler_handle: JoinHandle<Result<(), BenchClientError>> = tokio::spawn(async move {
            let broadcaster = Box::new(BackpressuredBroadcaster {});
            scheduler
                .run_with_broadcaster(scheduler_config, broadcaster)
                .await?;
            Ok(())
        });
        scheduler_handles.push(scheduler_handle);
    }

    // Single metrics reporter aggregating stats across all tpu-client-next instances.
    let successfully_sent_total = Arc::new(AtomicU64::new(0));
    let congestion_events_total = Arc::new(AtomicU64::new(0));
    let write_error_total = Arc::new(AtomicU64::new(0));
    tokio::spawn(report_aggregated_stats(
        all_stats,
        priority_fee_stats,
        METRICS_REPORTING_INTERVAL,
        successfully_sent_total.clone(),
        congestion_events_total.clone(),
        write_error_total.clone(),
        cancel,
    ));

    join_service(transaction_generator_task_handle, "TransactionGenerator").await?;

    if drain_seconds > 0 {
        info!("Generator finished; entering drain phase (max {drain_seconds}s).");
        drain_in_flight(
            successfully_sent_total.clone(),
            METRICS_REPORTING_INTERVAL,
            Duration::from_secs(drain_seconds),
            DRAIN_TAIL_AFTER_STABLE,
        )
        .await;
    }
    // Releasing these now closes the scheduler-side receivers and lets the
    // tpu-client-next instances shut down cleanly.
    drop(drain_senders);

    join_service(blockhash_task_handle, "BlockhashUpdater").await?;
    for (i, handle) in scheduler_handles.into_iter().enumerate() {
        let name = format!("Scheduler-{i}");
        join_service(handle, &name).await?;
    }

    info!(
        "tx-bench final: successfully_sent={} congestion_events={} write_error={}",
        successfully_sent_total.load(Ordering::Relaxed),
        congestion_events_total.load(Ordering::Relaxed),
        write_error_total.load(Ordering::Relaxed),
    );
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
