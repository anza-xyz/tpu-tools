use {
    crate::csv_writer::{CSVRecord, TransactionSendStatus},
    itertools::Itertools,
    log::{debug, warn},
    solana_clock::Slot,
    solana_measure::measure::Measure,
    solana_time_utils::timestamp,
    solana_tpu_client_next::{
        ConnectionWorkersSchedulerError, SendTransactionStats, WireTransaction,
        connection_workers_scheduler::{
            ConnectionWorkersSchedulerConfig, setup_endpoint, should_skip_current_leader,
        },
        workers_cache::{WorkersCache, WorkersCacheError, shutdown_worker},
    },
    solana_tpu_tools_common::leader_updater::LeaderUpdaterWithSlot,
    std::{net::SocketAddr, sync::Arc, time::Duration},
    tokio::time::interval,
    tokio_util::sync::CancellationToken,
};

pub async fn run_rate_latency_tool_scheduler<F, S>(
    rate: Duration,
    handshake_timeout: Duration,
    mut leader_updater: Box<dyn LeaderUpdaterWithSlot>,
    ConnectionWorkersSchedulerConfig {
        bind,
        stake_identity,
        num_connections,
        worker_channel_size,
        max_reconnect_attempts,
        leaders_fanout,
        override_initial_congestion_window,
    }: ConnectionWorkersSchedulerConfig,
    stats: Arc<SendTransactionStats>,
    cancel: CancellationToken,
    mut build_tx: F,
    mut send_record: S,
) -> Result<Arc<SendTransactionStats>, ConnectionWorkersSchedulerError>
where
    F: FnMut(Slot) -> (usize, WireTransaction, CSVRecord),
    S: FnMut(CSVRecord),
{
    assert!(
        worker_channel_size == 1,
        "Worker channel size must be 1 because otherwise we will wait when the channel has space."
    );
    let endpoint = setup_endpoint(bind, stake_identity, override_initial_congestion_window)?;

    debug!("Client endpoint bind address: {:?}", endpoint.local_addr());
    let mut workers = WorkersCache::new(num_connections, cancel.clone());

    let mut ticker = interval(rate);
    let mut next_leaders = Vec::with_capacity(leaders_fanout.connect);
    let mut connect_leaders = Vec::with_capacity(leaders_fanout.connect);
    let mut send_leaders = Vec::with_capacity(leaders_fanout.send);
    let main_loop = async {
        loop {
            ticker.tick().await;
            next_leaders.clear();
            let slot_estimate =
                leader_updater.next_leaders(leaders_fanout.connect, &mut next_leaders);
            let current_slot = slot_estimate
                .map(|estimate| estimate.slot)
                .unwrap_or_else(|| leader_updater.get_current_slot());
            select_unique_leaders(&next_leaders, leaders_fanout.connect, &mut connect_leaders);

            // add future leaders to the cache to hide the latency of opening
            // the connection.
            for peer in &connect_leaders {
                if let Some(evicted_worker) = workers.ensure_worker(
                    *peer,
                    &endpoint,
                    worker_channel_size,
                    max_reconnect_attempts,
                    handshake_timeout,
                    stats.clone(),
                ) {
                    shutdown_worker(evicted_worker);
                }
            }

            let rtt_ms = next_leaders.first().and_then(|peer| workers.rtt_ms(peer));
            let leader_window_end_ms =
                slot_estimate.and_then(|estimate| estimate.leader_window_end_ms);
            let skip_current = select_send_leaders(
                &next_leaders,
                leaders_fanout.send,
                leader_window_end_ms,
                rtt_ms,
                timestamp(),
                &mut send_leaders,
            );
            debug!(
                "Connect leaders: {connect_leaders:?}, send leaders: {send_leaders:?} for slot \
                 {current_slot}, leader_fanout: {leaders_fanout:?}, skip_current: {skip_current}, \
                 rtt_ms: {rtt_ms:?}, leader_window_end_ms: {leader_window_end_ms:?}."
            );

            // the time to generate and send the transaction < 70us, the
            // assumtion here is that the ticker interval >> this value  and
            // hence we can neglect generating/sending time for ticking.
            let mut measure_generate_send = Measure::start("generate_send");
            let (transaction_id, transaction, mut record) = build_tx(current_slot);
            for new_leader in &send_leaders {
                if !workers.contains(new_leader) {
                    warn!(
                        "No existing worker for {new_leader:?} (slot {current_slot}, skip sending \
                         to this leader."
                    );
                    continue;
                }

                let send_res =
                    workers.try_send_transaction_to_address(new_leader, transaction.clone());
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
    Ok(stats)
}

fn select_unique_leaders(
    leaders: &[SocketAddr],
    max_leaders: usize,
    selected_leaders: &mut Vec<SocketAddr>,
) {
    selected_leaders.clear();
    selected_leaders.extend(leaders.iter().take(max_leaders).copied().unique());
}

/// Uses the same arrival-time policy as Agave's transaction scheduler.
fn select_send_leaders(
    leaders: &[SocketAddr],
    max_leaders: usize,
    leader_window_end_ms: Option<u64>,
    rtt_ms: Option<u64>,
    now_ms: u64,
    selected_leaders: &mut Vec<SocketAddr>,
) -> bool {
    let skip_current = leaders.len() > 1
        && should_skip_current_leader(leader_window_end_ms, rtt_ms, now_ms);
    let candidates = if skip_current { &leaders[1..] } else { leaders };
    select_unique_leaders(candidates, max_leaders, selected_leaders);
    skip_current
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_leader_selection() {
        let a = SocketAddr::from(([127, 0, 0, 1], 8001));
        let b = SocketAddr::from(([127, 0, 0, 1], 8002));
        let c = SocketAddr::from(([127, 0, 0, 1], 8003));
        // With RTT 21 ms, estimated delivery takes ceil(21 / 2) + 50 = 61 ms.
        for (name, leaders, fanout, end_ms, rtt_ms, expected, skipped) in [
            ("before cutoff", vec![a, b, c], 2, Some(1062), Some(21), vec![a, b], false),
            ("at cutoff", vec![a, b, c], 2, Some(1061), Some(21), vec![b, c], true),
            ("expired without RTT", vec![a, b], 1, Some(999), None, vec![b], true),
            ("unknown RTT", vec![a, b], 1, Some(1001), None, vec![a], false),
            ("unknown timing", vec![a, b], 1, None, Some(21), vec![a], false),
            ("pinned", vec![a], 1, None, None, vec![a], false),
            ("no alternative", vec![a], 1, Some(999), Some(21), vec![a], false),
            ("no candidates", vec![], 1, Some(999), Some(21), vec![], false),
            ("repeated leader windows", vec![a, a, b], 2, Some(1061), Some(21), vec![a, b], true),
            ("deduplicate within fanout", vec![a, a, b], 2, None, None, vec![a], false),
            ("zero fanout", vec![a, b], 0, None, None, vec![], false),
        ] {
            let mut selected = vec![c];
            assert_eq!(
                select_send_leaders(&leaders, fanout, end_ms, rtt_ms, 1000, &mut selected),
                skipped,
                "{name}",
            );
            assert_eq!(selected, expected, "{name}");
        }
    }
}
