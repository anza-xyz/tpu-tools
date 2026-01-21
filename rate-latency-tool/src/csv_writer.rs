use {
    async_std::fs::File,
    csv_async::{AsyncWriterBuilder, QuoteStyle},
    log::{debug, error, info},
    solana_time_utils::timestamp,
    std::{
        collections::BTreeMap,
        path::PathBuf,
        sync::{Arc, Mutex},
        time::Duration,
    },
    thiserror::Error,
    tokio::{
        select,
        sync::mpsc::{unbounded_channel, Receiver, UnboundedReceiver},
        time::interval,
    },
    tokio_util::{sync::CancellationToken, task::TaskTracker},
};

#[derive(Debug, Error)]
pub enum CSVWriterError {
    #[error("CSVWriter stopped unexpectedly.")]
    WritingError,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionSendStatus {
    // Ok
    Sent,
    // Errors
    FullChannel,
    ReceiverDropped,
    Other,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CSVRecord {
    pub signature: String,
    pub transaction_id: Option<usize>,
    pub sent_slot: Option<u64>,
    pub received_slot: Option<u64>,
    pub sent_timestamp: Option<u64>,
    pub received_timestamp: Option<u64>,
    pub received_subscr_timestamp: Option<u64>,
    pub index_in_block: Option<u64>,
    pub tx_status: Vec<(TransactionSendStatus, String)>,
    pub tick: Option<u64>,
    pub tx_type: Option<String>,
    pub num_transaction_in_block: Option<usize>,
}

impl CSVRecord {
    fn field_names() -> Vec<&'static str> {
        vec![
            "signature",
            "transaction_id",
            "sent_slot",
            "received_slot",
            "sent_timestamp",
            "received_timestamp",
            "received_subscr_timestamp",
            "index_in_block",
            "tx_status",
            "tick",
            "tx_type",
            "num_transaction_in_block",
        ]
    }

    pub fn as_csv_record(&self) -> Vec<String> {
        vec![
            self.signature.clone(),
            self.transaction_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            self.sent_slot
                .map(|slot| slot.to_string())
                .unwrap_or_default(),
            self.received_slot
                .map(|slot| slot.to_string())
                .unwrap_or_default(),
            self.sent_timestamp
                .map(|ts| ts.to_string())
                .unwrap_or_default(),
            self.received_timestamp
                .map(|ts| ts.to_string())
                .unwrap_or_default(),
            self.received_subscr_timestamp
                .map(|ts| ts.to_string())
                .unwrap_or_default(),
            self.index_in_block
                .map(|ts| ts.to_string())
                .unwrap_or_default(),
            self.tx_status
                .iter()
                .map(|(status, address)| format!("{status:?}({address})"))
                .collect::<Vec<_>>()
                .join("|"),
            self.tick.map(|ts| ts.to_string()).unwrap_or_default(),
            self.tx_type.clone().unwrap_or_default(),
            self.num_transaction_in_block
                .map(|num| num.to_string())
                .unwrap_or_default(),
        ]
    }
}

/// Maximum age of transaction in milliseconds to keep it in the pending map.
/// Transaction has chance to be included in the block if its age is less than
/// 150 blocks because otherwise blockhash is invalid. I added here 25 slots
/// just to be sure that we don't flush it erroneously.
const MAX_TX_AGE_MS: u64 = (150 + 25) * 400;

/// Number of records to read at once from the channel to hide latency of channel operations.
const READ_AT_ONCE: usize = 16;
/// A larger number when writing to CSV to reduce the number of write calls.
const READ_AT_ONCE_FOR_CSV: usize = 128;

pub async fn run_csv_writer(
    file: PathBuf,
    mut block_tx_receiver: Receiver<CSVRecord>,
    mut tx_tracker_receiver: UnboundedReceiver<CSVRecord>,
    cancel: CancellationToken,
) -> Result<(), CSVWriterError> {
    let mut csv_writer = AsyncWriterBuilder::new()
        .quote_style(QuoteStyle::Never) // they are already quoted in the response
        .create_writer(
            File::create(file)
                .await
                .expect("Application should be able to create a file."),
        );

    csv_writer
        .write_record(CSVRecord::field_names())
        .await
        .expect("Should be able to write to csv");

    let pending_txs = Arc::new(Mutex::new(BTreeMap::<usize, CSVRecord>::new()));

    let (writer_sender, mut writer_receiver) = unbounded_channel();

    let tracker = TaskTracker::new();
    tracker.spawn({
        let cancel = cancel.clone();
        let pending_txs = pending_txs.clone();
        let writer_sender = writer_sender.clone();
        async move {
            let main_loop = async move {
                let mut complete_records = Vec::with_capacity(READ_AT_ONCE);
                loop {
                    let received = block_tx_receiver
                        .recv_many(&mut complete_records, READ_AT_ONCE)
                        .await;
                    debug!("CSV writer received {} records.", complete_records.len());
                    if received == 0 {
                        info!("Sender for records has been dropped. Stopping csv writer...");
                        break;
                    }

                    for mut record in complete_records.drain(..) {
                        if let Some(transaction_id) = record.transaction_id {
                            if let Some(pending) =
                                pending_txs.lock().unwrap().remove(&transaction_id)
                            {
                                record.tx_status = pending.tx_status;
                            }
                        }

                        if let Err(e) = writer_sender.send(record.as_csv_record()) {
                            error!("Write csv failed: {e}");
                        }
                    }
                }
            };
            cancel.run_until_cancelled(main_loop).await;
        }
    });

    let mut ticker = interval(Duration::from_millis(MAX_TX_AGE_MS));
    tracker.spawn({
        let pending_txs = pending_txs.clone();
        let writer_sender = writer_sender.clone();
        let cancel = cancel.clone();
        async move {
            let main_loop = async move {
                loop {
                    let _ = ticker.tick().await;
                    // pending txs are those which we sent, so by construction they have timestamp
                    let now = timestamp();
                    let mut to_remove = Vec::new();
                    let mut pending_txs = pending_txs.lock().unwrap();
                    for (id, rec) in pending_txs.iter() {
                        let age = now.saturating_sub(rec.sent_timestamp.unwrap());
                        if age > MAX_TX_AGE_MS {
                            if let Err(e) = writer_sender.send(rec.as_csv_record()) {
                                error!("Closed channel for writer: {e}");
                                break;
                            }
                            to_remove.push(*id);
                        } else {
                            // Since map is sorted by id (and in your case, by time),
                            // everything after this will be newer → stop iterating
                            break;
                        }
                    }
                    for id in to_remove {
                        pending_txs.remove(&id);
                    }
                }
            };
            cancel.run_until_cancelled(main_loop).await;
        }
    });

    tracker.spawn({
        let cancel = cancel.clone();
        let pending_txs = pending_txs.clone();
        async move {
        let mut sent_records = Vec::with_capacity(READ_AT_ONCE);
        loop {
            select! {
                received = tx_tracker_receiver.recv_many(&mut sent_records, READ_AT_ONCE) => {
                    debug!("CSV writer received {} records.", sent_records.len());
                    if received == 0 {
                        info!("Sender for records has been dropped. Stopping csv writer...");
                        break;
                    }

                    let mut pending_txs = pending_txs.lock().unwrap();

                    for record in sent_records.drain(..) {
                        // these transactions were sent, so we know that transaction_id is there
                        pending_txs.insert(record.transaction_id.unwrap(), record);
                    }
                }
                _ = cancel.cancelled() => {
                    info!("Cancellation received, dumping all pending txs and stopping tx tracker csv writer.");
                    // dump those which are still pending
                    let pending_txs = pending_txs.lock().unwrap();
                    for (_id, rec) in pending_txs.iter() {
                        if let Err(e) = writer_sender.send(rec.as_csv_record()) {
                            error!("Send csv failed: {e}");
                        }
                    }
                    break;
                }
            }
        }
    }});

    tracker.spawn(async move {
        let mut complete_records = Vec::with_capacity(READ_AT_ONCE_FOR_CSV);
        loop {
            let received = writer_receiver
                .recv_many(&mut complete_records, READ_AT_ONCE_FOR_CSV)
                .await;
            if received == 0 {
                info!("Writer channel closed. Stopping csv writer...");
                break;
            }
            for record in complete_records.drain(..) {
                if let Err(e) = csv_writer.write_record(record).await {
                    error!("Write csv failed: {e}");
                }
            }
            if let Err(e) = csv_writer.flush().await {
                error!("Flush csv failed: {e}");
                break;
            }
        }
    });

    tracker.close();
    debug!("Waiting for CSV writer tasks to finish...");
    tracker.wait().await;
    debug!("CSV writer tasks finished.");

    Ok(())
}
