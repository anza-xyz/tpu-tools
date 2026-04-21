use {
    crate::{
        backpressured_broadcaster::BackpressuredBroadcaster,
        cli::{EndpointConfig, ExecutionParams, TransactionParams},
        error::BenchClientError,
        generator::TransactionGenerator,
    },
    log::*,
    solana_keypair::Keypair,
    solana_net_utils::sockets::bind_to,
    solana_pubkey::Pubkey,
    solana_quic_definitions::{
        QUIC_MAX_STAKED_CONCURRENT_STREAMS, QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS,
        QUIC_MIN_STAKED_CONCURRENT_STREAMS, QUIC_TOTAL_STAKED_CONCURRENT_STREAMS,
    },
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_signer::{EncodableKey, Signer},
    solana_streamer::nonblocking::quic::ConnectionPeerType,
    solana_tpu_client_next::{
        Client, ClientBuilder, TransactionSender,
        node_address_service::LeaderTpuCacheServiceConfig, transaction_batch::TransactionBatch,
    },
    std::{fmt::Debug, sync::Arc, time::Duration},
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

pub async fn run_client(
    rpc_client: Arc<RpcClient>,
    websocket_url: String,
    accounts: AccountsFile,
    transaction_params: TransactionParams,
    execution_params: ExecutionParams,
) -> Result<(), BenchClientError> {
    let endpoint_configs = execution_params
        .resolved_endpoint_configs()
        .map_err(BenchClientError::InvalidCliArguments)?;
    let endpoint_identities = endpoint_configs
        .iter()
        .map(load_identity)
        .collect::<Result<Vec<_>, _>>()?;

    // Set up size of the txs batch to put into the queue to be equal to the num_streams_per_connection
    let mut num_streams_per_connection = usize::MAX;
    for (endpoint_config, validator_identity) in
        endpoint_configs.iter().zip(endpoint_identities.iter())
    {
        let endpoint_num_streams = compute_num_streams(
            &rpc_client,
            validator_identity.as_ref().map(|keypair| keypair.pubkey()),
        )
        .await
        .unwrap_or(DEFAULT_NUM_STREAMS_PER_CONNECTION);
        info!(
            "Endpoint {} will use {endpoint_num_streams} streams per connection.",
            endpoint_config.bind
        );
        num_streams_per_connection = num_streams_per_connection.min(endpoint_num_streams);
    }
    if num_streams_per_connection == usize::MAX {
        num_streams_per_connection = DEFAULT_NUM_STREAMS_PER_CONNECTION;
    }
    let tx_batch_size = transaction_params
        .simple_transfer_tx_params
        .tx_batch_size
        .map(|n| n.get());
    let send_batch_size = tx_batch_size.unwrap_or(num_streams_per_connection);
    info!("Number of streams per connection is {num_streams_per_connection}.");
    if let Some(tx_batch_size) = tx_batch_size {
        info!("Using tx batch size override: {tx_batch_size}.");
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

    // Use bounded to avoid producing too many batches of transactions.
    let (transaction_sender, transaction_receiver) = mpsc::channel(GENERATOR_CHANNEL_SIZE);

    let transaction_generator = TransactionGenerator::new(
        accounts,
        blockhash_receiver,
        transaction_sender,
        transaction_params,
        send_batch_size,
        execution_params.duration,
        execution_params.workers_pull_size,
    );

    let cancel = CancellationToken::new();
    let transaction_generator_task_handle =
        tokio::spawn(async move { transaction_generator.run().await });
    let leader_updater_config = LeaderTpuCacheServiceConfig {
        lookahead_leaders: 4,
        refresh_nodes_info_every: Duration::from_secs(30),
        max_consecutive_failures: 5,
    };
    let (transaction_senders, clients) = build_clients(
        rpc_client.clone(),
        websocket_url,
        &execution_params,
        endpoint_configs,
        endpoint_identities,
        leader_updater_config,
        cancel.clone(),
    )
    .await?;
    let dispatcher_handle =
        tokio::spawn(
            async move { dispatch_batches(transaction_receiver, transaction_senders).await },
        );

    join_service(transaction_generator_task_handle, "TransactionGenerator").await?;
    join_service(blockhash_task_handle, "BlockhashUpdater").await?;
    join_service(dispatcher_handle, "Dispatcher").await?;
    shutdown_clients(clients).await?;
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

async fn build_clients(
    rpc_client: Arc<RpcClient>,
    websocket_url: String,
    execution_params: &ExecutionParams,
    endpoint_configs: Vec<EndpointConfig>,
    endpoint_identities: Vec<Option<Keypair>>,
    leader_updater_config: LeaderTpuCacheServiceConfig,
    cancel: CancellationToken,
) -> Result<(Vec<TransactionSender>, Vec<Client>), BenchClientError> {
    let mut transaction_senders = Vec::with_capacity(endpoint_configs.len());
    let mut clients = Vec::with_capacity(endpoint_configs.len());

    for (endpoint_config, validator_identity) in endpoint_configs
        .into_iter()
        .zip(endpoint_identities.into_iter())
    {
        let leader_updater = create_leader_updater(
            rpc_client.clone(),
            execution_params.leader_tracker.clone(),
            leader_updater_config.clone(),
            websocket_url.clone(),
            cancel.child_token(),
        )
        .await?;

        let bind_socket =
            bind_to(endpoint_config.bind.ip(), endpoint_config.bind.port()).map_err(|err| {
                BenchClientError::InvalidCliArguments(format!(
                    "failed to bind endpoint {}: {err}",
                    endpoint_config.bind
                ))
            })?;

        let (transaction_sender, client) = ClientBuilder::new(leader_updater)
            .runtime_handle(tokio::runtime::Handle::current())
            .bind_socket(bind_socket)
            .leader_send_fanout(execution_params.send_fanout)
            .identity(validator_identity.as_ref())
            .max_cache_size(execution_params.num_max_open_connections)
            .worker_channel_size(WORKER_CHANNEL_SIZE)
            .sender_channel_size(GENERATOR_CHANNEL_SIZE)
            .max_reconnect_attempts(MAX_RECONNECT_ATTEMPTS)
            .cancel_token(cancel.child_token())
            .broadcaster(BackpressuredBroadcaster {})
            .metric_reporter(|stats, cancel| async move {
                stats
                    .report_to_influxdb(
                        "transaction-bench-network",
                        METRICS_REPORTING_INTERVAL,
                        cancel,
                    )
                    .await;
            })
            .build()?;

        transaction_senders.push(transaction_sender);
        clients.push(client);
    }

    Ok((transaction_senders, clients))
}

async fn dispatch_batches(
    mut transaction_receiver: mpsc::Receiver<TransactionBatch>,
    transaction_senders: Vec<TransactionSender>,
) -> Result<(), BenchClientError> {
    let mut next_sender = 0usize;
    let sender_count = transaction_senders.len();

    while let Some(transaction_batch) = transaction_receiver.recv().await {
        let wired_transactions: Vec<_> = transaction_batch.into_iter().collect();
        transaction_senders[next_sender]
            .send_transactions_in_batch(wired_transactions)
            .await?;
        next_sender = next_sender.saturating_add(1);
        if next_sender == sender_count {
            next_sender = 0;
        }
    }

    Ok(())
}

async fn shutdown_clients(clients: Vec<Client>) -> Result<(), BenchClientError> {
    for client in clients {
        client.shutdown().await?;
    }
    Ok(())
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
