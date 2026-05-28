//! Checkout the `README.md` for the guidance.
use {
    log::*,
    solana_cli_config::ConfigInput,
    solana_keypair::Keypair,
    solana_metrics::datapoint_info,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_signer::{EncodableKey, Signer},
    solana_tpu_tools_common::{
        accounts_file::{
            AccountsFile, create_ephemeral_accounts, create_file_persisted_accounts,
            read_accounts_file,
        },
        cli::LeaderTracker,
    },
    solana_transaction_bench::{
        cli::{ClientCliParameters, Command, build_cli_parameters},
        error::BenchClientError,
        mock_rpc_client::new_mock_rpc_client,
        run_client::{RunClientStats, run_client},
    },
    std::{sync::Arc, time::Duration},
    tokio::{sync::oneshot, task::JoinHandle},
    tokio_util::sync::CancellationToken,
};

/// How often tpu-client-next reports network metrics.
const METRICS_REPORTING_INTERVAL: Duration = Duration::from_secs(1);

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

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
    validate_mock_rpc_usage(&parameters)?;

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

    let rpc_client = Arc::new(if parameters.mock_rpc {
        new_mock_rpc_client(parameters.commitment_config)
    } else {
        RpcClient::new_with_commitment(
            parameters.json_rpc_url.to_string(),
            parameters.commitment_config,
        )
    });

    match parameters.command {
        Command::Run {
            transaction_params,
            account_params,
            execution_params,
        } => {
            let accounts = if parameters.mock_rpc {
                info!(
                    "Skipping payer account creation because --mock-rpc is enabled; generating \
                     local ephemeral payers instead."
                );
                create_mock_accounts(account_params.num_payers)
            } else {
                create_ephemeral_accounts(
                    rpc_client.clone(),
                    authority,
                    account_params.num_payers,
                    account_params.payer_account_balance,
                    parameters.validate_accounts,
                )
                .await?
            };
            let cancel = CancellationToken::new();
            let (stats_sender, stats_receiver) = oneshot::channel();
            let metrics_task = spawn_metrics_reporter(stats_receiver, cancel.clone());
            let result = run_client(
                rpc_client,
                websocket_url,
                accounts,
                transaction_params,
                execution_params,
                Some(stats_sender),
                cancel.clone(),
            )
            .await;
            finish_client_run(result, cancel, metrics_task).await?;
        }
        Command::ReadAccountsRun {
            read_accounts,
            transaction_params,
            execution_params,
        } => {
            let accounts = read_accounts_file(read_accounts.accounts_file.clone());
            let cancel = CancellationToken::new();
            let (stats_sender, stats_receiver) = oneshot::channel();
            let metrics_task = spawn_metrics_reporter(stats_receiver, cancel.clone());
            let result = run_client(
                rpc_client,
                websocket_url,
                accounts,
                transaction_params,
                execution_params,
                Some(stats_sender),
                cancel.clone(),
            )
            .await;
            finish_client_run(result, cancel, metrics_task).await?;
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

fn validate_mock_rpc_usage(parameters: &ClientCliParameters) -> Result<(), BenchClientError> {
    if !parameters.mock_rpc {
        return Ok(());
    }

    if parameters.validate_accounts {
        return Err(BenchClientError::InvalidCliArguments(
            "--mock-rpc does not support --validate-accounts".to_string(),
        ));
    }

    let requires_pinned = matches!(
        &parameters.command,
        Command::Run {
            execution_params,
            ..
        } | Command::ReadAccountsRun {
            execution_params,
        ..
        } if !matches!(
            &execution_params.leader_tracker,
            LeaderTracker::PinnedLeaderTracker { .. }
        )
    );

    if requires_pinned {
        return Err(BenchClientError::InvalidCliArguments(
            "--mock-rpc requires `pinned-leader-tracker`".to_string(),
        ));
    }

    if matches!(&parameters.command, Command::WriteAccounts(_)) {
        return Err(BenchClientError::InvalidCliArguments(
            "--mock-rpc does not support `write-accounts`".to_string(),
        ));
    }

    Ok(())
}

fn create_mock_accounts(num_payers: usize) -> AccountsFile {
    AccountsFile {
        payers: (0..num_payers).map(|_| Keypair::new()).collect(),
    }
}

fn spawn_metrics_reporter(
    stats_receiver: oneshot::Receiver<RunClientStats>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        match stats_receiver.await {
            Ok(client_stats) => {
                report_aggregated_stats(client_stats, METRICS_REPORTING_INTERVAL, cancel).await;
            }
            Err(_) => {
                debug!("Stats reporter was not started because stats were not available.");
            }
        }
    })
}

/// Periodically reads and resets stats from all scheduler instances, sums them,
/// and reports the aggregate to InfluxDB under a single metric name.
#[allow(clippy::arithmetic_side_effects)]
async fn report_aggregated_stats(
    client_stats: RunClientStats,
    reporting_interval: Duration,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(reporting_interval);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let (mut connect_error, mut connection_error, mut successfully_sent,
                     mut congestion_events, mut write_error) = (0i64, 0i64, 0i64, 0i64, 0i64);
                for stats in &client_stats.send_transaction_stats {
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

                let (total_priority_fees, priority_fee_tx_count) =
                    client_stats.priority_fee_stats.read_and_reset();
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

async fn finish_client_run(
    result: Result<(), BenchClientError>,
    cancel: CancellationToken,
    metrics_task: JoinHandle<()>,
) -> Result<(), BenchClientError> {
    cancel.cancel();
    if let Err(err) = metrics_task.await {
        error!("Stats reporting task panicked: {err:?}");
        result?;
        return Err(BenchClientError::TaskJoinFailure {
            task_name: "StatsReporter".to_string(),
            reason: err.to_string(),
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_commitment_config::CommitmentConfig,
        solana_tpu_tools_common::cli::{AccountParams, WriteAccounts},
        solana_transaction_bench::cli::{
            ExecutionParams, InstructionPaddingParams, PriorityFeeParams, SimpleTransferTxParams,
            TransactionParams,
        },
        std::net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    fn test_run_parameters(leader_tracker: LeaderTracker) -> ClientCliParameters {
        ClientCliParameters {
            json_rpc_url: "http://localhost:8899".to_string(),
            commitment_config: CommitmentConfig::confirmed(),
            authority: None,
            validate_accounts: false,
            mock_rpc: true,
            command: Command::Run {
                account_params: AccountParams {
                    num_payers: 4,
                    payer_account_balance: 1_000,
                },
                execution_params: ExecutionParams {
                    staked_identity_files: vec![],
                    bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                    duration: None,
                    target_tps: None,
                    num_max_open_connections: 1,
                    workers_pull_size: 1,
                    send_fanout: 1,
                    compute_unit_price: None,
                    priority_fee_params: PriorityFeeParams {
                        random_compute_unit_price_max: 0,
                        priority_fee_schedule_period_ms: None,
                    },
                    leader_tracker,
                },
                transaction_params: TransactionParams {
                    simple_transfer_tx_params: SimpleTransferTxParams {
                        lamports_to_transfer: 513,
                        transfer_tx_cu_budget: 600,
                        num_send_instructions_per_tx: 1,
                        tx_batch_size: None,
                        num_conflict_groups: None,
                    },
                    padding_params: InstructionPaddingParams {
                        instruction_padding_data_size: None,
                        instruction_padding_program_id: None,
                    },
                    use_txv1: false,
                },
            },
        }
    }

    #[test]
    fn test_mock_rpc_requires_pinned_leader_tracker() {
        let parameters = test_run_parameters(LeaderTracker::WsLeaderTracker);

        let err = validate_mock_rpc_usage(&parameters)
            .unwrap_err()
            .to_string();
        assert!(err.contains("pinned-leader-tracker"));
    }

    #[test]
    fn test_mock_rpc_rejects_write_accounts() {
        let parameters = ClientCliParameters {
            json_rpc_url: "http://localhost:8899".to_string(),
            commitment_config: CommitmentConfig::confirmed(),
            authority: None,
            validate_accounts: false,
            mock_rpc: true,
            command: Command::WriteAccounts(WriteAccounts {
                accounts_file: "accounts.json".into(),
                account_params: AccountParams {
                    num_payers: 4,
                    payer_account_balance: 1_000,
                },
            }),
        };

        let err = validate_mock_rpc_usage(&parameters)
            .unwrap_err()
            .to_string();
        assert!(err.contains("write-accounts"));
    }

    #[test]
    fn test_create_mock_accounts_generates_requested_number_of_payers() {
        let accounts = create_mock_accounts(3);
        assert_eq!(accounts.payers.len(), 3);
    }
}
