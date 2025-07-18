use {
    async_std::fs::File,
    csv_async::{AsyncWriterBuilder, QuoteStyle},
    log::{debug, error, info},
    std::path::PathBuf,
    thiserror::Error,
    tokio::{select, sync::mpsc::Receiver},
    tokio_util::sync::CancellationToken,
};

#[derive(Debug, Error)]
pub enum CSVWriterError {
    #[error("CSVWriter stopped unexpectedly.")]
    WritingError,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CSVRecord {
    pub signature: String,
    pub transaction_id: usize,
    pub sent_slot: u64,
    pub received_slot: u64,
    pub sent_timestamp: u64,
    pub received_timestamp: Option<u64>,
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
        ]
    }

    pub fn as_csv_record(&self) -> Vec<String> {
        vec![
            self.signature.clone(),
            self.transaction_id.to_string(),
            self.sent_slot.to_string(),
            self.received_slot.to_string(),
            self.sent_timestamp.to_string(),
            self.received_timestamp
                .map(|ts| ts.to_string())
                .unwrap_or_default(),
        ]
    }
}

pub async fn run_csv_writer(
    file: PathBuf,
    mut receiver: Receiver<CSVRecord>,
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

    let mut records = Vec::with_capacity(16);
    let mut result = Ok(());

    loop {
        select! {
            received = receiver.recv_many(&mut records, 16) => {
                debug!("CSV writer received {} records.", records.len());
                if received == 0 {
                    info!("Sender for records has been dropped. Stopping csv writer...");
                    break;
                }

                for record in records.drain(..) {
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
            _ = cancel.cancelled() => {
                break;
            }
        }
    }
    if let Err(e) = csv_writer.flush().await {
        error!("Flush CSV writer should succeed, error instead is: {e}");
        result = Err(CSVWriterError::WritingError);
    }

    result
}
