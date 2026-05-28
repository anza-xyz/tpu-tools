//! Checkout the `README.md` for the guidance.
use {
    log::*,
    solana_cli_config::ConfigInput,
    solana_keypair::Keypair,
    solana_rate_latency_tool::{
        cli::{ClientCliParameters, Command, build_cli_parameters},
        error::RateLatencyToolError,
        run_client::run_client,
    },
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_signer::{EncodableKey, Signer},
    solana_tpu_client_next::SendTransactionStats,
    solana_tpu_tools_common::accounts_file::{
        create_ephemeral_accounts, create_file_persisted_accounts, read_accounts_file,
    },
    std::{sync::Arc, time::Duration},
    tokio_util::sync::CancellationToken,
};

/// How often tpu-client-next reports network metrics.
const METRICS_REPORTING_INTERVAL: Duration = Duration::from_secs(1);

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
            let stats = Arc::new(SendTransactionStats::default());
            let metrics_task = spawn_metrics_reporter(stats.clone(), cancel.clone());
            let result = run_client(
                rpc_client,
                websocket_url,
                accounts,
                execution_params,
                analysis_params,
                stats,
                cancel.clone(),
            )
            .await;
            finish_client_run(result, cancel, metrics_task).await?;
        }
        Command::ReadAccountsRun {
            read_accounts,
            execution_params,
            analysis_params,
        } => {
            let accounts = read_accounts_file(read_accounts.accounts_file.clone());
            let stats = Arc::new(SendTransactionStats::default());
            let metrics_task = spawn_metrics_reporter(stats.clone(), cancel.clone());
            let result = run_client(
                rpc_client,
                websocket_url,
                accounts,
                execution_params,
                analysis_params,
                stats,
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

fn spawn_metrics_reporter(
    stats: Arc<SendTransactionStats>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        stats
            .report_to_influxdb(
                "rate-latency-tool-network",
                METRICS_REPORTING_INTERVAL,
                cancel,
            )
            .await;
    })
}

async fn finish_client_run(
    result: Result<(), RateLatencyToolError>,
    cancel: CancellationToken,
    metrics_task: tokio::task::JoinHandle<()>,
) -> Result<(), RateLatencyToolError> {
    cancel.cancel();
    if let Err(err) = metrics_task.await {
        error!("Stats reporting task panicked: {err:?}");
        result?;
        return Err(RateLatencyToolError::UnexpectedError);
    }
    result
}
