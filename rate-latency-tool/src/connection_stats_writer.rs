use {
    async_std::fs::File,
    csv_async::{AsyncWriterBuilder, QuoteStyle},
    log::{debug, error},
    solana_time_utils::timestamp,
    solana_tpu_client_next::send_transaction_stats::SendTransactionStatsNonAtomic,
    std::{net::SocketAddr, path::PathBuf},
    thiserror::Error,
    tokio::sync::mpsc::UnboundedReceiver,
};

#[derive(Debug, Error)]
pub enum ConnectionStatsWriterError {
    #[error("Connection stats writer stopped unexpectedly.")]
    WritingError,
}

#[derive(Debug, PartialEq)]
pub struct ConnectionStatsRecord {
    pub timestamp: u64,
    pub close_reason: &'static str,
    pub validator_pubkey: String,
    pub tpu_address: SocketAddr,
    pub stats: SendTransactionStatsNonAtomic,
}

impl ConnectionStatsRecord {
    pub fn new(
        close_reason: &'static str,
        validator_pubkey: String,
        tpu_address: SocketAddr,
        stats: SendTransactionStatsNonAtomic,
    ) -> Self {
        Self {
            timestamp: timestamp(),
            close_reason,
            validator_pubkey,
            tpu_address,
            stats,
        }
    }

    fn field_names() -> Vec<&'static str> {
        vec![
            "timestamp",
            "close_reason",
            "validator_pubkey",
            "tpu_address",
            "successfully_sent",
            "connect_error_cids_exhausted",
            "connect_error_invalid_remote_address",
            "connect_error_other",
            "connection_error_application_closed",
            "connection_error_cids_exhausted",
            "connection_error_connection_closed",
            "connection_error_locally_closed",
            "connection_error_reset",
            "connection_error_timed_out",
            "connection_error_transport_error",
            "connection_error_version_mismatch",
            "write_error_closed_stream",
            "transport_congestion_events",
            "write_error_connection_lost",
            "write_error_stopped",
            "write_error_zero_rtt_rejected",
        ]
    }

    pub fn as_csv_record(&self) -> Vec<String> {
        vec![
            self.timestamp.to_string(),
            self.close_reason.to_string(),
            self.validator_pubkey.clone(),
            self.tpu_address.to_string(),
            self.stats.successfully_sent.to_string(),
            self.stats.connect_error_cids_exhausted.to_string(),
            self.stats.connect_error_invalid_remote_address.to_string(),
            self.stats.connect_error_other.to_string(),
            self.stats.connection_error_application_closed.to_string(),
            self.stats.connection_error_cids_exhausted.to_string(),
            self.stats.connection_error_connection_closed.to_string(),
            self.stats.connection_error_locally_closed.to_string(),
            self.stats.connection_error_reset.to_string(),
            self.stats.connection_error_timed_out.to_string(),
            self.stats.connection_error_transport_error.to_string(),
            self.stats.connection_error_version_mismatch.to_string(),
            self.stats.write_error_closed_stream.to_string(),
            self.stats.transport_congestion_events.to_string(),
            self.stats.write_error_connection_lost.to_string(),
            self.stats.write_error_stopped.to_string(),
            self.stats.write_error_zero_rtt_rejected.to_string(),
        ]
    }
}

pub async fn run_connection_stats_writer(
    file: PathBuf,
    mut receiver: UnboundedReceiver<ConnectionStatsRecord>,
) -> Result<(), ConnectionStatsWriterError> {
    let mut csv_writer = AsyncWriterBuilder::new()
        .quote_style(QuoteStyle::Never)
        .create_writer(
            File::create(file)
                .await
                .map_err(|_| ConnectionStatsWriterError::WritingError)?,
        );

    csv_writer
        .write_record(ConnectionStatsRecord::field_names())
        .await
        .map_err(|_| ConnectionStatsWriterError::WritingError)?;

    while let Some(record) = receiver.recv().await {
        if let Err(err) = csv_writer.write_record(record.as_csv_record()).await {
            debug!("Failed to write connection stats to csv: {err}");
        }
        if let Err(err) = csv_writer.flush().await {
            error!("Flush connection stats csv failed: {err}");
            return Err(ConnectionStatsWriterError::WritingError);
        }
    }

    csv_writer
        .flush()
        .await
        .map_err(|_| ConnectionStatsWriterError::WritingError)?;
    Ok(())
}
