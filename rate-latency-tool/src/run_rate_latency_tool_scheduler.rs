use {
    crate::csv_writer::{CSVRecord, TransactionSendStatus},
    log::{debug, warn},
    solana_clock::Slot,
    solana_measure::measure::Measure,
    solana_tpu_client_next::{
        connection_workers_scheduler::{setup_endpoint, ConnectionWorkersSchedulerConfig},
        leader_updater::LeaderUpdater,
        transaction_batch::TransactionBatch,
        workers_cache::{shutdown_worker, spawn_worker, WorkersCache, WorkersCacheError},
        ConnectionWorkersSchedulerError, SendTransactionStats,
    },
    std::{sync::Arc, time::Duration},
    tokio::time::interval,
    tokio_util::sync::CancellationToken,
};

pub trait LeaderSlotEstimator {
    fn get_current_slot(&mut self) -> Slot;
}

pub trait LeaderUpdaterWithSlot: LeaderUpdater + LeaderSlotEstimator {}
impl<T> LeaderUpdaterWithSlot for T where T: LeaderUpdater + LeaderSlotEstimator {}

pub async fn run_rate_latency_tool_scheduler<F, S>(
    rate: Duration,
    handshake_timeout: Duration,
    mut leader_updater: Box<dyn LeaderUpdaterWithSlot>,
    ConnectionWorkersSchedulerConfig {
        bind,
        stake_identity,
        num_connections,
        skip_check_transaction_age,
        worker_channel_size,
        max_reconnect_attempts,
        leaders_fanout,
    }: ConnectionWorkersSchedulerConfig,
    stats: Arc<SendTransactionStats>,
    cancel: CancellationToken,
    mut build_tx: F,
    mut send_record: S,
) -> Result<Arc<SendTransactionStats>, ConnectionWorkersSchedulerError>
where
    F: FnMut(Slot) -> (usize, Vec<u8>, CSVRecord),
    S: FnMut(CSVRecord),
{
    assert!(
        worker_channel_size == 1,
        "Worker channel size must be 1 because otherwise we will wait when the channel has space."
    );
    let endpoint = setup_endpoint(bind, stake_identity)?;

    debug!("Client endpoint bind address: {:?}", endpoint.local_addr());
    let mut workers = WorkersCache::new(num_connections, cancel.clone());

    let mut ticker = interval(rate);
    let main_loop = async {
        loop {
            ticker.tick().await;
            let current_slot = leader_updater.get_current_slot();

            let connect_leaders = leader_updater.next_leaders(leaders_fanout.connect);
            let send_leaders = leader_updater.next_leaders(leaders_fanout.send);
            //extract_send_leaders(&connect_leaders, leaders_fanout.send);
            debug!(
                "Connect leaders: {connect_leaders:?}, send leaders: {send_leaders:?} for slot \
                 {current_slot}, leader_fanout: {leaders_fanout:?}."
            );

            // add future leaders to the cache to hide the latency of opening
            // the connection.
            for peer in connect_leaders {
                debug!("Checking connection to the peer: {peer}");
                if !workers.contains(&peer) {
                    debug!("Creating connection to the peer: {peer}");
                    let worker = spawn_worker(
                        &endpoint,
                        &peer,
                        worker_channel_size,
                        skip_check_transaction_age,
                        max_reconnect_attempts,
                        handshake_timeout,
                        stats.clone(),
                    );
                    if let Some(pop_worker) = workers.push(peer, worker) {
                        shutdown_worker(pop_worker)
                    }
                } else {
                    debug!("Connection to the peer {peer} exists.");
                }
            }

            // the time to generate and send the transaction < 70us, the
            // assumtion here is that the ticker interval >> this value  and
            // hence we can neglect generating/sending time for ticking.
            let mut measure_generate_send = Measure::start("generate_send");
            let (transaction_id, memo_tx, mut record) = build_tx(current_slot);
            let transaction_batch = TransactionBatch::new(vec![memo_tx]);
            for new_leader in &send_leaders {
                if !workers.contains(new_leader) {
                    warn!(
                        "No existing worker for {new_leader:?} (slot {current_slot}, skip sending \
                         to this leader."
                    );
                    continue;
                }

                let send_res =
                    workers.try_send_transactions_to_address(new_leader, transaction_batch.clone());
                let status = match send_res {
                    Ok(()) => {
                        debug!(
                            "Succefully sent transaction with id: {transaction_id}, current slot: \
                             {current_slot}, leader: {new_leader}."
                        );
                        TransactionSendStatus::Sent
                    }
                    Err(WorkersCacheError::ShutdownError) => {
                        debug!(
                            "Failed with ShutdownError sending transaction with id: \
                             {transaction_id}, current slot: {current_slot}, leader: {new_leader}."
                        );
                        TransactionSendStatus::Other
                    }
                    Err(WorkersCacheError::ReceiverDropped) => {
                        debug!(
                            "Failed with ReceiverDropped sending transaction with id: \
                             {transaction_id}, current slot: {current_slot}, leader: {new_leader}."
                        );
                        // Remove the worker from the cache, if the peer has disconnected.
                        if let Some(pop_worker) = workers.pop(*new_leader) {
                            shutdown_worker(pop_worker)
                        }
                        TransactionSendStatus::ReceiverDropped
                    }
                    Err(WorkersCacheError::FullChannel) => {
                        debug!(
                            "Failed with FullChannel sending transaction with id: \
                             {transaction_id}, current slot: {current_slot}, leader: {new_leader}."
                        );
                        TransactionSendStatus::FullChannel
                    }
                    Err(err) => {
                        debug!(
                            "Failed with {err} sending transaction with id: {transaction_id}, \
                             current slot: {current_slot}, leader: {new_leader}."
                        );
                        TransactionSendStatus::Other
                    }
                };
                record.tx_status.push((status, new_leader.to_string()));
            }
            send_record(record);
            measure_generate_send.stop();
            debug!(
                "Generated and sent transaction batch in {} us",
                measure_generate_send.as_us()
            );
        }
    };
    tokio::select! {
        () = main_loop => (),
        () = cancel.cancelled() => (),
    }

    workers.shutdown().await;

    endpoint.close(0u32.into(), b"Closing connection");
    leader_updater.stop().await;
    Ok(stats)
}
