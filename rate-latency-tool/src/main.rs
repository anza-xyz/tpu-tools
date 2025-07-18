//! Checkout the `README.md` for the guidance.
use {
    log::*,
    solana_cli_config::ConfigInput,
    solana_keypair::Keypair,
    solana_rate_latency_tool::accounts_file::{
        create_ephemeral_accounts, create_file_persisted_accounts, read_accounts_file,
    },
    solana_rate_latency_tool::{
        cli::{build_cli_parameters, ClientCliParameters, Command},
        error::RateLatencyToolError,
        run_client::run_client,
    },
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_signer::{EncodableKey, Signer},
    std::sync::Arc,
    tokio_util::sync::CancellationToken,
};

fn main() {
    solana_logger::setup_with_default("solana=info");

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
async fn run(parameters: ClientCliParameters) -> Result<(), RateLatencyToolError> {
    let authority = if let Some(authority_file) = parameters.authority {
        Keypair::read_from_file(authority_file)
            .map_err(|_err| RateLatencyToolError::KeypairReadFailure)?
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
    let cancel = CancellationToken::new();

    match parameters.command {
        Command::Run {
            account_params,
            execution_params,
            analysis_params,
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
                execution_params,
                analysis_params,
                cancel,
            )
            .await?;
        }
        Command::ReadAccountsRun {
            read_accounts,
            execution_params,
            analysis_params,
        } => {
            let accounts = read_accounts_file(read_accounts.accounts_file.clone());
            run_client(
                rpc_client,
                websocket_url,
                accounts,
                execution_params,
                analysis_params,
                cancel,
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
