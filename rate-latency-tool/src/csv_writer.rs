use {
    async_std::fs::File,
    csv_async::{AsyncWriterBuilder, QuoteStyle},
    log::{debug, error, info},
    solana_time_utils::timestamp,
    std::{collections::BTreeMap, path::PathBuf, time::Duration},
    thiserror::Error,
    tokio::{
        select,
        sync::mpsc::{Receiver, UnboundedReceiver},
        time::interval,
    },
    tokio_util::sync::CancellationToken,
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
    pub transaction_id: usize,
    pub sent_slot: u64,
    pub received_slot: Option<u64>,
    pub sent_timestamp: u64,
    pub received_timestamp: Option<u64>,
    pub received_subscr_timestamp: Option<u64>,
    pub index_in_block: Option<u64>,
    pub tx_status: Vec<(TransactionSendStatus, String)>,
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
        ]
    }

    pub fn as_csv_record(&self) -> Vec<String> {
        vec![
            self.signature.clone(),
            self.transaction_id.to_string(),
            self.sent_slot.to_string(),
            self.received_slot
                .map(|slot| slot.to_string())
                .unwrap_or_default(),
            self.sent_timestamp.to_string(),
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
        ]
    }
}

/// Maximum age of transaction in milliseconds to keep it in the pending map.
/// Transaction has chance to be included in the block if its age is less than
/// 150 blocks because otherwise blockhash is invalid. I added here 25 slots
/// just to be sure that we don't flush it erroneously.
const MAX_TX_AGE_MS: u64 = (150 + 25) * 400;

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

    let mut complete_records = Vec::with_capacity(16);
    let mut sent_records = Vec::with_capacity(16);
    let mut result = Ok(());
    let mut pending_txs = BTreeMap::<usize, CSVRecord>::new();

    let mut ticker = interval(Duration::from_millis(MAX_TX_AGE_MS));

    loop {
        select! {
            received = block_tx_receiver.recv_many(&mut complete_records, 16) => {
                debug!("CSV writer received {} records.", complete_records.len());
                if received == 0 {
                    info!("Sender for records has been dropped. Stopping csv writer...");
                    break;
                }

                for mut record in complete_records.drain(..) {
                    if let Some(pending) = pending_txs.remove(&record.transaction_id) {
                        record.tx_status = pending.tx_status;
                    }

                    if let Err(e) = csv_writer.write_record(record.as_csv_record()).await {
                        error!("Write csv failed: {e}");
                        result = Err(CSVWriterError::WritingError);
                    }
                }
                if let Err(e) = csv_writer.flush().await {
                    error!("Flush csv failed: {e}");
                    result = Err(CSVWriterError::WritingError);
                    break;
                }
            }
            received = tx_tracker_receiver.recv_many(&mut sent_records, 16) => {
                debug!("CSV writer received {} records.", sent_records.len());
                if received == 0 {
                    info!("Sender for records has been dropped. Stopping csv writer...");
                    break;
                }

                for record in sent_records.drain(..) {
                    pending_txs.insert(record.transaction_id, record);
                }
            }
            _ = ticker.tick() => {
                let now = timestamp();
                let mut to_remove = Vec::new();
                for (id, rec) in pending_txs.iter() {
                    let age = now.saturating_sub(rec.sent_timestamp);
                    if age > MAX_TX_AGE_MS {
                        if let Err(e) = csv_writer.write_record(rec.as_csv_record()).await {
                            error!("Write csv failed: {e}");
                            result = Err(CSVWriterError::WritingError);
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
                if let Err(e) = csv_writer.flush().await {
                    error!("Flush csv failed: {e}");
                    result = Err(CSVWriterError::WritingError);
                    break;
                }
            }
            _ = cancel.cancelled() => {
                break;
            }
        }
    }
    // dump those which are still pending
    for (_id, rec) in pending_txs.iter() {
        if let Err(e) = csv_writer.write_record(rec.as_csv_record()).await {
            error!("Write csv failed: {e}");
            result = Err(CSVWriterError::WritingError);
        }
    }
    if let Err(e) = csv_writer.flush().await {
        error!("Flush CSV writer should succeed, error instead is: {e}");
        result = Err(CSVWriterError::WritingError);
    }

    result
}
