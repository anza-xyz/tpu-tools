//! This module provider writer to csv file to save number of transactions per
//! block. Why I save this information separately: because it might arrive after
//! information about transaction so at the time of writing tx we might not have
//! information about number of txs in the block. But having two files allows to
//! do it later in post-processing.

use {
    crate::error::RateLatencyToolError,
    async_std::fs::File,
    csv_async::{AsyncWriterBuilder, QuoteStyle},
    log::{debug, error},
    solana_clock::Slot,
    std::path::PathBuf,
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
};

pub struct TxsPerBlockWriter;

impl TxsPerBlockWriter {
    pub async fn run(
        file: PathBuf,
        mut num_txs_per_block_receiver: mpsc::Receiver<(Slot, usize)>,
        cancel: CancellationToken,
    ) -> Result<(), RateLatencyToolError> {
        let mut csv_writer = AsyncWriterBuilder::new()
            .quote_style(QuoteStyle::Never) // they are already quoted in the response
            .create_writer(
                File::create(file)
                    .await
                    .expect("Application should be able to create a file."),
            );

        csv_writer
            .write_record(vec!["slot", "num_txs_per_block"])
            .await
            .expect("Should be able to write to csv");
        loop {
            tokio::select! {
                data = num_txs_per_block_receiver.recv() => {
                    if let Some((slot, num_txs)) = data {
                        if csv_writer.write_record(vec![slot.to_string(), num_txs.to_string()]).await.is_err() {
                            debug!("Failed to write number of transactions per block to csv.");
                        }
                        if let Err(e) = csv_writer.flush().await {
                            error!("Flush csv failed: {e}");
                            break;
                        }
                    } else {
                        break;
                    }
                }
                _ = cancel.cancelled() => {
                    debug!("Number of transactions per block task noticed cancellation and exited.");
                    break;
                }
            }
        }
        if let Err(e) = csv_writer.flush().await {
            error!("Flush csv failed: {e}");
        }
        Ok(())
    }
}
