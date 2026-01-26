//! Checkout the `README.md` for the guidance.
use {
    log::*,
    solana_cli_config::ConfigInput,
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
        ConnectionWorkersScheduler,
        connection_workers_scheduler::{
            BindTarget, ConnectionWorkersSchedulerConfig, Fanout, StakeIdentity,
        },
        node_address_service::LeaderTpuCacheServiceConfig,
    },
    solana_transaction_bench::{
        backpressured_broadcaster::BackpressuredBroadcaster,
        cli::{
            ClientCliParameters, Command, ExecutionParams, TransactionParams, build_cli_parameters,
        },
        error::BenchClientError,
        generator::TransactionGenerator,
    },
    std::{fmt::Debug, sync::Arc, time::Duration},
    tokio::{
        sync::{mpsc, watch},
        task::JoinHandle,
    },
    tokio_util::sync::CancellationToken,
    tools_common::{
        accounts_file::{
            AccountsFile, create_ephemeral_accounts, create_file_persisted_accounts,
            read_accounts_file,
        },
        blockhash_updater::BlockhashUpdater,
        leader_updater::create_leader_updater,
    },
};

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

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

fn main() {
    agave_logger::setup_with_default("solana=info");

    let opt = build_cli_parameters();
    let code = {
        if let Err(e) = run(opt) {
            error!("ERROR: {e}");
            1
        } else {
            0
        }
    };
    ::std::process::exit(code);
}

#[tokio::main]
async fn run(parameters: ClientCliParameters) -> Result<(), BenchClientError> {
    let authority = if let Some(authority_file) = parameters.authority {
        Keypair::read_from_file(authority_file)
            .map_err(|_err| BenchClientError::KeypairReadFailure)?
    } else {
        // create authority just for this run
        Keypair::new()
    };
    info!("Use authority {}", authority.pubkey());

    let (_, websocket_url) =
        ConfigInput::compute_websocket_url_setting("", "", &parameters.json_rpc_url, "");

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        parameters.json_rpc_url.to_string(),
        parameters.commitment_config,
    ));

    match parameters.command {
        Command::Run {
            transaction_params,
            account_params,
            execution_params,
        } => {
            let accounts = create_ephemeral_accounts(
                rpc_client.clone(),
                authority,
                account_params.num_payers,
                account_params.payer_account_balance,
                parameters.validate_accounts,
            )
            .await?;
            run_client(
                rpc_client,
                websocket_url,
                accounts,
                transaction_params,
                execution_params,
            )
            .await?;
        }
        Command::ReadAccountsRun {
            read_accounts,
            transaction_params,
            execution_params,
        } => {
            let accounts = read_accounts_file(read_accounts.accounts_file.clone());
            run_client(
                rpc_client,
                websocket_url,
                accounts,
                transaction_params,
                execution_params,
            )
            .await?;
        }
        Command::WriteAccounts(write_accounts) => {
            create_file_persisted_accounts(
                rpc_client.clone(),
                authority,
                write_accounts.accounts_file,
                write_accounts.account_params.num_payers,
                write_accounts.account_params.payer_account_balance,
                parameters.validate_accounts,
            )
            .await?;
        }
    }

    Ok(())
}

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

async fn join_service<Error>(handle: JoinHandle<Result<(), Error>>, task_name: &str)
where
    Error: Debug,
{
    match handle.await {
        Ok(Ok(_)) => info!("Task {task_name} completed successfully"),
        Ok(Err(e)) => error!("Task failed with error: {e:?}"),
        Err(e) => error!("Task was cancelled or panicked: {e:?}"),
    }
}

async fn run_client(
    rpc_client: Arc<RpcClient>,
    websocket_url: String,
    accounts: AccountsFile,
    transaction_params: TransactionParams,
    ExecutionParams {
        staked_identity_file,
        bind,
        duration,
        num_max_open_connections,
        workers_pull_size,
        send_fanout,
        //TODO(klykov): pass to tx generator
        compute_unit_price: _,
        leader_tracker,
    }: ExecutionParams,
) -> Result<(), BenchClientError> {
    let validator_identity = if let Some(staked_identity_file) = staked_identity_file {
        Some(
            Keypair::read_from_file(staked_identity_file)
                .map_err(|_err| BenchClientError::KeypairReadFailure)?,
        )
    } else {
        None
    };

    // Set up size of the txs batch to put into the queue to be equal to the num_streams_per_connection
    let num_streams_per_connection = compute_num_streams(
        &rpc_client,
        validator_identity.as_ref().map(|keypair| keypair.pubkey()),
    )
    .await
    .unwrap_or(DEFAULT_NUM_STREAMS_PER_CONNECTION);
    let send_batch_size = num_streams_per_connection;
    info!("Number of streams per connection is {num_streams_per_connection}.");

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
        duration,
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
    let leader_updater = create_leader_updater(
        rpc_client.clone(),
        leader_tracker,
        config,
        websocket_url,
        cancel.clone(),
    )
    .await?;

    let scheduler_handle: JoinHandle<Result<(), BenchClientError>> = tokio::spawn(async move {
        let config = ConnectionWorkersSchedulerConfig {
            bind: BindTarget::Address(bind),
            stake_identity: validator_identity.map(|ident| StakeIdentity::new(&ident)),
            num_connections: num_max_open_connections,
            worker_channel_size: WORKER_CHANNEL_SIZE,
            max_reconnect_attempts: MAX_RECONNECT_ATTEMPTS,
            leaders_fanout: Fanout {
                send: send_fanout,
                connect: send_fanout.saturating_add(1),
            },
            skip_check_transaction_age: false,
        };

        let (_, update_identity_receiver) = watch::channel(None);
        let scheduler = ConnectionWorkersScheduler::new(
            leader_updater,
            transaction_receiver,
            update_identity_receiver,
            cancel.clone(),
        );
        // leaking handle to this task, as it will run until the cancel signal is received
        tokio::spawn(scheduler.get_stats().report_to_influxdb(
            "transaction-bench-network",
            METRICS_REPORTING_INTERVAL,
            cancel,
        ));

        let broadcaster = Box::new(BackpressuredBroadcaster {});
        scheduler.run_with_broadcaster(config, broadcaster).await?;
        Ok(())
    });

    join_service(transaction_generator_task_handle, "TransactionGenerator").await;
    join_service(blockhash_task_handle, "BlockhashUpdater").await;
    join_service::<BenchClientError>(scheduler_handle, "Scheduler").await;
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
