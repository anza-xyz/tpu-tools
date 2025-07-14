use {
    crate::{
        accounts_file::AccountsFile, blockhash_updater::BlockhashUpdater, cli::ExecutionParams,
        error::RateLatencyToolError, leader_updater::create_leader_updater,
        run_rate_latency_tool_scheduler::run_rate_latency_tool_scheduler,
    },
    log::*,
    solana_clock::Slot,
    solana_compute_budget_interface::ComputeBudgetInstruction,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_signer::{EncodableKey, Signer},
    solana_time_utils::timestamp,
    solana_tpu_client_next::{
        connection_workers_scheduler::{
            BindTarget, ConnectionWorkersSchedulerConfig, Fanout, StakeIdentity,
        },
        SendTransactionStats,
    },
    solana_transaction::Transaction,
    std::{fmt::Debug, sync::Arc, time::Duration},
    tokio::{sync::watch, task::JoinHandle, time::sleep},
    tokio_util::sync::CancellationToken,
};

/// How often tpu-client-next reports network metrics.
const METRICS_REPORTING_INTERVAL: Duration = Duration::from_secs(1);

pub async fn run_client(
    rpc_client: Arc<RpcClient>,
    websocket_url: String,
    accounts: AccountsFile,
    ExecutionParams {
        staked_identity_file,
        bind,
        duration,
        num_max_open_connections,
        send_fanout,
        send_interval: rate,
        compute_unit_price,
    }: ExecutionParams,
    cancel: CancellationToken,
) -> Result<(), RateLatencyToolError> {
    let validator_identity = if let Some(staked_identity_file) = staked_identity_file {
        Some(
            Keypair::read_from_file(staked_identity_file)
                .map_err(|_err| RateLatencyToolError::KeypairReadFailure)?,
        )
    } else {
        None
    };

    if let Some(application_timeout) = duration {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            sleep(application_timeout).await;
            info!("Timeout reached, cancelling...");
            cancel.cancel();
        });
    }

    let blockhash = rpc_client
        .get_latest_blockhash()
        .await
        .expect("Blockhash request should not fail.");
    let (blockhash_sender, blockhash_receiver) = watch::channel(blockhash);
    let blockhash_updater = BlockhashUpdater::new(rpc_client.clone(), blockhash_sender);

    let blockhash_task_handle = tokio::spawn(async move { blockhash_updater.run().await });

    let leader_updater = create_leader_updater(rpc_client.clone(), websocket_url).await?;

    let scheduler_handle: JoinHandle<Result<(), RateLatencyToolError>> = tokio::spawn(async move {
        let config = ConnectionWorkersSchedulerConfig {
            bind: BindTarget::Address(bind),
            stake_identity: validator_identity.map(|ident| StakeIdentity::new(&ident)),
            num_connections: num_max_open_connections,
            // If worker is busy sending previous transaction, better drop the
            // current one because timestamp will be driffted.
            worker_channel_size: 1,
            // No need to reconnect if the first attempt failed.
            max_reconnect_attempts: 0,
            leaders_fanout: Fanout {
                send: send_fanout,
                connect: send_fanout.saturating_add(1),
            },
            skip_check_transaction_age: true,
        };

        let stats = Arc::new(SendTransactionStats::default());
        let mut payer_iter = accounts.payers.iter().cycle();
        let mut tx_id: usize = 0;
        let scheduler = run_rate_latency_tool_scheduler(
            rate,
            leader_updater,
            config,
            stats.clone(),
            cancel.clone(),
            |current_slot| {
                let payer = payer_iter.next().unwrap();
                let blockhash = *blockhash_receiver.borrow();
                let timestamp = timestamp();
                let copy_tx_id = tx_id;
                tx_id = tx_id.wrapping_add(1);
                create_memo_transaction(
                    copy_tx_id,
                    current_slot,
                    timestamp,
                    compute_unit_price,
                    payer,
                    blockhash,
                )
            },
        );

        // leaking handle to this task, as it will run until the cancel signal is received
        tokio::spawn(stats.report_to_influxdb(
            "transaction-bench-network",
            METRICS_REPORTING_INTERVAL,
            cancel,
        ));

        scheduler.await?;
        Ok(())
    });

    join_service(blockhash_task_handle, "BlockhashUpdater").await;
    join_service::<RateLatencyToolError>(scheduler_handle, "Scheduler").await;
    Ok(())
}

async fn join_service<Error>(handle: JoinHandle<Result<(), Error>>, task_name: &str)
where
    Error: Debug,
{
    match handle.await {
        Ok(Ok(_)) => info!("Task {task_name} completed successfully"),
        Ok(Err(e)) => error!("Task failed with error: {:?}", e),
        Err(e) => error!("Task was cancelled or panicked: {:?}", e),
    }
}

fn create_memo_transaction(
    tx_id: usize,
    current_slot: Slot,
    timestamp: u64,
    compute_unit_price: Option<u64>,
    payer: &Keypair,
    blockhash: Hash,
) -> Vec<u8> {
    const MEMO_TX_CU: u32 = 10_000;
    let memo = format!("{tx_id},{current_slot},{timestamp}");
    let mut instructions = vec![];
    instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(MEMO_TX_CU));
    if let Some(compute_unit_price) = compute_unit_price {
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
            compute_unit_price,
        ));
    }
    instructions.push(spl_memo::build_memo(memo.as_bytes(), &[]));

    let tx = Transaction::new_signed_with_payer(
        &instructions,
        Some(&payer.pubkey()),
        &[&payer],
        blockhash,
    );
    bincode::serialize(&tx).unwrap()
}
