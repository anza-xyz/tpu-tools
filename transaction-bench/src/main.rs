//! Checkout the `README.md` for the guidance.
use {
    log::*,
    solana_cli_config::ConfigInput,
    solana_keypair::Keypair,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_signer::{EncodableKey, Signer},
    solana_tpu_tools_common::accounts_file::{
        AccountsFile, create_ephemeral_accounts, create_file_persisted_accounts,
        read_accounts_file,
    },
    solana_transaction_bench::{
        cli::{ClientCliParameters, Command, build_cli_parameters},
        error::BenchClientError,
        mock_rpc_client::new_mock_rpc_client,
        run_client::run_client,
    },
    std::sync::Arc,
    tools_common::cli::LeaderTracker,
};

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

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_commitment_config::CommitmentConfig,
        solana_transaction_bench::cli::{
            ExecutionParams, InstructionPaddingParams, PriorityFeeParams, SimpleTransferTxParams,
            TransactionParams,
        },
        std::net::{IpAddr, Ipv4Addr, SocketAddr},
        tools_common::cli::{AccountParams, WriteAccounts},
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
