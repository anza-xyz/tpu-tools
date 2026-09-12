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
    solana_tpu_tools_common::{
        accounts_creator::create_tpu_account_creation_client,
        accounts_file::{
            create_ephemeral_accounts_with_sender, create_file_persisted_accounts_with_sender,
            read_accounts_file,
        },
        cli::LeaderTracker,
    },
    std::{num::NonZeroUsize, sync::Arc, time::Duration},
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
            let account_creation_client = create_tpu_account_creation_client(
                rpc_client.clone(),
                execution_params.leader_tracker.clone(),
                websocket_url.clone(),
                execution_params.bind,
                execution_params.staked_identity_file.clone(),
                NonZeroUsize::new(execution_params.num_max_open_connections)
                    .expect("num-max-open-connections must be non-zero"),
                execution_params.send_fanout,
                cancel.child_token(),
            )
            .await?;
            let accounts = create_ephemeral_accounts_with_sender(
                rpc_client.clone(),
                authority,
                account_params.num_payers,
                account_params.payer_account_balance,
                parameters.validate_accounts,
                account_creation_client.transaction_sender.clone(),
            )
            .await;
            account_creation_client.shutdown().await?;
            let accounts = accounts?;
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
            let account_creation_client = create_tpu_account_creation_client(
                rpc_client.clone(),
                LeaderTracker::WsLeaderTracker,
                websocket_url,
                "0.0.0.0:0"
                    .parse()
                    .expect("default bind address should be valid"),
                None,
                NonZeroUsize::new(16).expect("default max open connections must be non-zero"),
                1,
                cancel.child_token(),
            )
            .await?;
            let result = create_file_persisted_accounts_with_sender(
                rpc_client.clone(),
                authority,
                write_accounts.accounts_file,
                write_accounts.account_params.num_payers,
                write_accounts.account_params.payer_account_balance,
                parameters.validate_accounts,
                account_creation_client.transaction_sender.clone(),
            )
            .await;
            account_creation_client.shutdown().await?;
            result?;
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
