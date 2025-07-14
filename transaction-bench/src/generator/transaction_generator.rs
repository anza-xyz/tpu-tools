//! Service generating serialized transactions in batches.
use {
    crate::{
        cli::{SimpleTransferTxParams, TransactionParams},
        generator::simple_transfers_generator::generate_transfer_transaction_batch,
    },
    log::*,
    solana_hash::Hash,
    solana_measure::measure::Measure,
    solana_tpu_client_next::transaction_batch::TransactionBatch,
    std::sync::Arc,
    thiserror::Error,
    tokio::{
        sync::{mpsc::Sender, watch},
        task::JoinSet,
        time::{Duration, Instant},
    },
    tools_common::accounts_file::AccountsFile,
};

const COMPUTE_BUDGET_INSTRUCTION_CU_COST: u32 = 150;
const SIMPLE_TRANSFER_INSTRUCTION_CU_COST: u32 = 150;

#[derive(Error, Debug)]
pub enum TransactionGeneratorError {
    #[error("Transactions receiver has been dropped unexpectedly.")]
    ReceiverDropped,

    #[error("Failed to generate transaction batch.")]
    GenerateTxBatchFailure,
}

pub struct TransactionGenerator {
    accounts: AccountsFile,
    blockhash_receiver: watch::Receiver<Hash>,
    transactions_sender: Sender<TransactionBatch>,
    transaction_params: TransactionParams,
    send_batch_size: usize,
    run_duration: Option<Duration>,
    workers_pull_size: usize,
}

impl TransactionGenerator {
    pub fn new(
        accounts: AccountsFile,
        blockhash_receiver: watch::Receiver<Hash>,
        transactions_sender: Sender<TransactionBatch>,
        transaction_params: TransactionParams,
        send_batch_size: usize,
        duration: Option<Duration>,
        workers_pull_size: usize,
    ) -> Self {
        Self {
            accounts,
            blockhash_receiver,
            transactions_sender,
            transaction_params,
            send_batch_size,
            run_duration: duration,
            workers_pull_size,
        }
    }

    #[allow(clippy::arithmetic_side_effects)]
    pub async fn run(self) -> Result<(), TransactionGeneratorError> {
        let payers = Arc::new(self.accounts.payers);
        let len_payers = payers.len();
        let mut index_payer: usize = 0;
        let mut futures = JoinSet::new();

        //TODO(klykov): extract to function
        // Validate inputs that couldn't be done in CLI due to interdependency between two CLI args.
        // Ensure CU budget is sufficient for multi-instruction transfer transactions.
        let &SimpleTransferTxParams {
            num_send_instructions_per_tx,
            transfer_tx_cu_budget,
            ..
        } = &self.transaction_params.simple_transfer_tx_params;

        let transfer_tx_min_cu_budget = COMPUTE_BUDGET_INSTRUCTION_CU_COST
            + SIMPLE_TRANSFER_INSTRUCTION_CU_COST * num_send_instructions_per_tx as u32;

        if transfer_tx_cu_budget < transfer_tx_min_cu_budget {
            error!(
                "Insufficient CU budget for transfer transaction: set to {transfer_tx_cu_budget}, \
                 need at least {transfer_tx_min_cu_budget}.\nSet cli argument \
                 --transfer_tx_cu_budget to {transfer_tx_min_cu_budget}",
            );
            return Err(TransactionGeneratorError::GenerateTxBatchFailure);
        }

        let start = Instant::now();
        loop {
            if let Some(run_duration) = self.run_duration {
                if start.elapsed() >= run_duration {
                    info!("Transaction generator is stopping...");
                    while let Some(result) = futures.join_next().await {
                        debug!("Future result {result:?}");
                    }
                    break;
                }
            }
            if self.transactions_sender.is_closed() {
                return Err(TransactionGeneratorError::ReceiverDropped);
            }
            let blockhash = *self.blockhash_receiver.borrow();

            while futures.len() < self.workers_pull_size {
                let send_batch_size = self.send_batch_size;
                let transaction_params = self.transaction_params.clone();
                let payers = payers.clone();
                let transactions_sender = self.transactions_sender.clone();
                let transaction_type = TransactionType::Transfer;

                match transaction_type {
                    TransactionType::Transfer => {
                        let num_send_instructions_per_tx = transaction_params
                            .simple_transfer_tx_params
                            .num_send_instructions_per_tx;
                        futures.spawn(async move {
                            let Ok(wired_tx_batch) = generate_transfer_transaction_batch(
                                payers,
                                index_payer,
                                blockhash,
                                transaction_params.simple_transfer_tx_params,
                                send_batch_size,
                            )
                            .await
                            else {
                                warn!("Failed to generate transfer txs batch!");
                                return;
                            };

                            send_batch(wired_tx_batch, transactions_sender).await;
                        });
                        // 2 * self.send_batch_size because for simple transfer
                        // we form transactions as follows: p1 -> p2, p3 -> p4, etc.
                        index_payer = index_payer.saturating_add(
                            2 * num_send_instructions_per_tx * self.send_batch_size,
                        ) % len_payers;
                    }
                }
            }
            futures.join_next().await;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TransactionType {
    Transfer,
    //TODO(klykov): add memo
}

async fn send_batch(wired_txs_batch: Vec<Vec<u8>>, transactions_sender: Sender<TransactionBatch>) {
    let mut measure_send_to_queue = Measure::start("add transaction batch to channel");
    if let Err(err) = transactions_sender
        .send(TransactionBatch::new(wired_txs_batch))
        .await
    {
        error!("Receiver dropped, error {err}.");
        return;
    }
    measure_send_to_queue.stop();
    debug!(
        "Time to send into transactions queue: {} us",
        measure_send_to_queue.as_us()
    );
}
