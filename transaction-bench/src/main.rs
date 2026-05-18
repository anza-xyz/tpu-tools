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
