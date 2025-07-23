use {
    crate::{
        accounts_file::AccountsFile,
        blockhash_updater::BlockhashUpdater,
        cli::{ExecutionParams, TxAnalysisParams},
        csv_writer::run_csv_writer,
        error::RateLatencyToolError,
        leader_updater::create_leader_updater,
        run_rate_latency_tool_scheduler::run_rate_latency_tool_scheduler,
        yellowstone_subscriber::run_yellowstone_subscriber,
    },
    log::*,
    solana_clock::Slot,
    solana_compute_budget_interface::ComputeBudgetInstruction,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
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
    std::{sync::Arc, time::Duration},
    tokio::{
        sync::{mpsc, watch},
        task::JoinSet,
        time::sleep,
    },
    tokio_util::sync::CancellationToken,
};

const CSV_RECORD_CHANNEL_SIZE: usize = 128;

/// Memo transcaction CU price depends on the message size, but generally it is
/// in range 9000-20000.
const MEMO_TX_CU: u32 = 20_000;

/// How often tpu-client-next reports network metrics.
const METRICS_REPORTING_INTERVAL: Duration = Duration::from_secs(1);

/// How long after stop sending transactions, we want to receive updates from
/// yellowstone.
const YELLOWSTONE_STREAM_SHUTDOWN_DELAY: Duration = Duration::from_secs(2);

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
        pinned_address,
    }: ExecutionParams,
    TxAnalysisParams {
        output_csv_file,
        yellowstone_url,
    }: TxAnalysisParams,
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

    let mut tasks = JoinSet::<Result<(), RateLatencyToolError>>::new();

    // If yellowstone is active, we want to receive transactions for some time
    // after we stop sending them for the case that there is a delay between
    // transaction sending and receiving them from subscription.
    let wait_for_yellowstone_longer = output_csv_file.is_some();
    if let Some(output_csv_file) = output_csv_file {
        let yellowstone_url = yellowstone_url
            .expect("yellowstone-url should be required in cla when csv-file specified.");
        let (csv_sender, csv_receiver) = mpsc::channel(CSV_RECORD_CHANNEL_SIZE);
        tasks.spawn({
            let cancel = cancel.clone();
            async move {
                run_csv_writer(output_csv_file, csv_receiver, cancel.clone()).await?;
                Ok(())
            }
        });

        let account_pubkeys: Vec<Pubkey> = accounts
            .payers
            .iter()
            .map(|keypair| keypair.pubkey())
            .collect();
        let cancel = cancel.clone();
        tasks.spawn(async move {
            run_yellowstone_subscriber(&yellowstone_url, &account_pubkeys, csv_sender, cancel)
                .await?;
            Ok(())
        });
    }

    let cancel_tx_sending = cancel.child_token();
    if let Some(application_timeout) = duration {
        let cancel = cancel.clone();
        let cancel_tx_sending = cancel_tx_sending.clone();
        tasks.spawn(async move {
            tokio::select! {
                _ = sleep(application_timeout) => {
                    info!("Timeout reached, stop sending...");
                    if wait_for_yellowstone_longer {
                        cancel_tx_sending.cancel();
                        sleep(YELLOWSTONE_STREAM_SHUTDOWN_DELAY).await;
                        info!("Timeout reached, stop receiving yellowstone updates...");
                    }
                    cancel.cancel();
                }
                _ = cancel.cancelled() => {
                    debug!("Timeout task noticed cancellation early and exited.");
                }
            }
            Ok(())
        });
    }

    let blockhash = rpc_client
        .get_latest_blockhash()
        .await
        .expect("Blockhash request should not fail.");
    let (blockhash_sender, blockhash_receiver) = watch::channel(blockhash);
    let blockhash_updater = BlockhashUpdater::new(rpc_client.clone(), blockhash_sender);

    tasks.spawn(async move {
        blockhash_updater.run().await?;
        Ok(())
    });

    let leader_updater =
        create_leader_updater(rpc_client.clone(), websocket_url, pinned_address).await?;

    let stats = Arc::new(SendTransactionStats::default());
    tasks.spawn({
        let stats = stats.clone();
        let cancel = cancel_tx_sending.clone();
        async move {
            stats
                .report_to_influxdb(
                    "rate-latency-tool-network",
                    METRICS_REPORTING_INTERVAL,
                    cancel,
                )
                .await;
            Ok(())
        }
    });

    tasks.spawn({
        let cancel = cancel_tx_sending.clone();
        async move {
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

            let mut payer_iter = accounts.payers.iter().cycle();
            let mut tx_id: usize = 0;
            let scheduler = run_rate_latency_tool_scheduler(
                rate,
                leader_updater,
                config,
                stats.clone(),
                cancel,
                |current_slot| {
                    let payer = payer_iter.next().unwrap();
                    let blockhash = *blockhash_receiver.borrow();
                    let timestamp = timestamp();
                    let copy_tx_id = tx_id;
                    tx_id = tx_id.wrapping_add(1);
                    (
                        copy_tx_id,
                        create_memo_transaction(
                            copy_tx_id,
                            current_slot,
                            timestamp,
                            compute_unit_price,
                            payer,
                            blockhash,
                        ),
                    )
                },
            );

            scheduler.await?;
            debug!("Scheduler stopped.");
            Ok(())
        }
    });

    let mut result = Ok(());
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(_)) => info!("Task completed successfully"),
            Ok(Err(e)) => {
                error!("Task failed with error: {:?}, stoppting the tool...", e);
                result = Err(e);
                cancel.cancel();
            }
            Err(e) => {
                error!("Task panicked: {:?}, stoppting the tool...", e);
                result = Err(RateLatencyToolError::UnexpectedError);
                cancel.cancel();
            }
        }
    }
    result
}

fn create_memo_transaction(
    tx_id: usize,
    current_slot: Slot,
    timestamp: u64,
    compute_unit_price: Option<u64>,
    payer: &Keypair,
    blockhash: Hash,
) -> Vec<u8> {
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
