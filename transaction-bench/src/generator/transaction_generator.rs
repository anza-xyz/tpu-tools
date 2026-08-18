//! Service generating serialized transactions in batches.
use {
    crate::{
        cli::{SimpleTransferTxParams, TransactionParams},
        error::BenchClientError,
        generator::simple_transfers_generator::{SharedSlice, generate_transfer_transaction_batch},
        priority_fee::{PriorityFeeMode, PriorityFeeStats},
    },
    log::*,
    rand::{rng, seq::SliceRandom},
    solana_hash::Hash,
    solana_measure::measure::Measure,
    solana_tpu_client_next::WireTransaction,
    solana_tpu_tools_common::accounts_file::AccountsFile,
    std::{num::NonZeroUsize, sync::Arc},
    thiserror::Error,
    tokio::{
        sync::{mpsc::Sender, watch},
        task::JoinSet,
        time::{Duration, Instant},
    },
};

const COMPUTE_BUDGET_INSTRUCTION_CU_COST: u32 = 150;
const SIMPLE_TRANSFER_INSTRUCTION_CU_COST: u32 = 150;
const PADDED_TRANSFER_INSTRUCTION_CU_COST: u32 = 3_000;

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
    transactions_senders: Vec<Sender<WireTransaction>>,
    transaction_params: TransactionParams,
    compute_unit_price: Option<u64>,
    priority_fee_mode: PriorityFeeMode,
    priority_fee_stats: Arc<PriorityFeeStats>,
    run_duration: Option<Duration>,
    num_transactions: Option<u64>,
    target_tps: Option<u64>,
    generate_tx_batch_size: usize,
    workers_pull_size: usize,
}

impl TransactionGenerator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        accounts: AccountsFile,
        blockhash_receiver: watch::Receiver<Hash>,
        transactions_senders: Vec<Sender<WireTransaction>>,
        transaction_params: TransactionParams,
        compute_unit_price: Option<u64>,
        priority_fee_mode: PriorityFeeMode,
        priority_fee_stats: Arc<PriorityFeeStats>,
        duration: Option<Duration>,
        num_transactions: Option<u64>,
        target_tps: Option<u64>,
        generate_tx_batch_size: usize,
        workers_pull_size: usize,
    ) -> Self {
        Self {
            accounts,
            blockhash_receiver,
            transactions_senders,
            transaction_params,
            compute_unit_price,
            priority_fee_mode,
            priority_fee_stats,
            run_duration: duration,
            num_transactions,
            target_tps,
            generate_tx_batch_size,
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
        let transfer_tx_cu_budget = self
            .transaction_params
            .simple_transfer_tx_params
            .transfer_tx_cu_budget;

        let transfer_tx_min_cu_budget =
            compute_transfer_tx_min_cu_budget(&self.transaction_params, self.compute_unit_price);

        if transfer_tx_cu_budget < transfer_tx_min_cu_budget {
            error!(
                "Insufficient CU budget for transfer transaction: set to {transfer_tx_cu_budget}, \
                 need at least {transfer_tx_min_cu_budget}.\nSet cli argument \
                 --transfer-tx-cu-budget to {transfer_tx_min_cu_budget}",
            );
            return Err(TransactionGeneratorError::GenerateTxBatchFailure);
        }

        let lamports_pool_size = self
            .transaction_params
            .simple_transfer_tx_params
            .max_lamports_to_transfer as usize;
        let mut lamports_pool = create_lamports_pool(lamports_pool_size);
        let mut lamports_index: usize = 0;

        let num_senders = self.transactions_senders.len();
        let mut sender_index: usize = 0;
        let start = Instant::now();
        let mut next_batch_at = self.target_tps.map(|_| start);
        let mut txs_scheduled: u64 = 0;
        loop {
            if let Some(stop_reason) = stop_reason(
                self.run_duration,
                self.num_transactions,
                start.elapsed(),
                txs_scheduled,
            ) {
                info!("Transaction generator is stopping: {stop_reason}.");
                while let Some(result) = futures.join_next().await {
                    debug!("Future result {result:?}");
                }
                break;
            }

            if self.transactions_senders.iter().all(|s| s.is_closed()) {
                return Err(TransactionGeneratorError::ReceiverDropped);
            }
            let blockhash = *self.blockhash_receiver.borrow();

            while futures.len() < self.workers_pull_size {
                if let Some(next_batch_deadline) = next_batch_at {
                    tokio::time::sleep_until(next_batch_deadline).await;
                }
                let Some(send_batch_size) = next_batch_size(
                    self.num_transactions,
                    self.generate_tx_batch_size,
                    txs_scheduled,
                ) else {
                    break;
                };
                txs_scheduled = txs_scheduled.saturating_add(send_batch_size as u64);
                let transaction_params = self.transaction_params.clone();
                let compute_unit_price = self.compute_unit_price;
                let priority_fee_mode = self.priority_fee_mode.clone();
                let priority_fee_stats = self.priority_fee_stats.clone();
                let payers = payers.clone();
                let transactions_sender = self.transactions_senders[sender_index].clone();
                sender_index = (sender_index + 1) % num_senders;
                let transaction_type = TransactionType::Transfer;

                match transaction_type {
                    TransactionType::Transfer => {
                        let num_send_instructions_per_tx = transaction_params
                            .simple_transfer_tx_params
                            .num_send_instructions_per_tx;
                        let total_pairs = num_send_instructions_per_tx * send_batch_size;
                        let num_conflict_groups = transaction_params
                            .simple_transfer_tx_params
                            .num_conflict_groups;
                        let lamports = next_lamports_slice(
                            &mut lamports_pool,
                            &mut lamports_index,
                            total_pairs,
                        );
                        futures.spawn(async move {
                            let Ok(wired_tx_batch) = generate_transfer_transaction_batch(
                                payers,
                                index_payer,
                                lamports,
                                blockhash,
                                transaction_params,
                                compute_unit_price,
                                priority_fee_mode,
                                priority_fee_stats,
                                send_batch_size,
                            )
                            .await
                            else {
                                warn!("Failed to generate transfer txs batch!");
                                return;
                            };

                            send_batch(wired_tx_batch, transactions_sender).await;
                        });

                        let receivers_consumed =
                            num_conflict_groups.map(|g| g.get()).unwrap_or(total_pairs);

                        // accounts_from consumes `total_pairs`, accounts_to consumes `receivers_consumed`
                        index_payer = index_payer.saturating_add(total_pairs + receivers_consumed)
                            % len_payers;
                    }
                }

                if let Some(target_tps) = self.target_tps {
                    let batch_interval = compute_batch_interval(send_batch_size, target_tps);
                    next_batch_at = Some(
                        next_batch_at
                            .map(|next_batch_deadline| next_batch_deadline + batch_interval)
                            .unwrap_or_else(|| Instant::now() + batch_interval),
                    );
                }
            }
            futures.join_next().await;
        }
        Ok(())
    }
}

#[allow(clippy::arithmetic_side_effects)]
fn compute_transfer_tx_min_cu_budget(
    transaction_params: &TransactionParams,
    compute_unit_price: Option<u64>,
) -> u32 {
    let &SimpleTransferTxParams {
        num_send_instructions_per_tx,
        ..
    } = &transaction_params.simple_transfer_tx_params;

    let use_instruction_padding = transaction_params
        .padding_params
        .instruction_padding_data_size
        .is_some();
    let per_instruction_cu_cost = if use_instruction_padding {
        PADDED_TRANSFER_INSTRUCTION_CU_COST
    } else {
        SIMPLE_TRANSFER_INSTRUCTION_CU_COST
    };

    // Legacy transfer transactions include explicit compute-budget instructions.
    // V1 transfer transactions encode these settings in Message config.
    let compute_budget_instructions_count = if transaction_params.use_txv1 {
        0
    } else {
        1 + u32::from(compute_unit_price.is_some()) + u32::from(use_instruction_padding)
    };

    compute_budget_instructions_count * COMPUTE_BUDGET_INSTRUCTION_CU_COST
        + per_instruction_cu_cost * num_send_instructions_per_tx as u32
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TransactionType {
    Transfer,
    //TODO(klykov): add memo
}

async fn send_batch(
    wired_txs_batch: Vec<WireTransaction>,
    transactions_sender: Sender<WireTransaction>,
) {
    let mut measure_send_to_queue = Measure::start("add transaction batch to channel");
    for wired_tx in wired_txs_batch {
        if let Err(err) = transactions_sender.send(wired_tx).await {
            error!("Receiver dropped, error {err}.");
            return;
        }
    }
    measure_send_to_queue.stop();
    debug!(
        "Time to send into transactions queue: {} us",
        measure_send_to_queue.as_us()
    );
}

/// Returns the reason the generator should stop, if a configured limit
/// (`--duration` or `--num-transactions`) has been reached.
fn stop_reason(
    run_duration: Option<Duration>,
    num_transactions: Option<u64>,
    elapsed: Duration,
    txs_scheduled: u64,
) -> Option<&'static str> {
    if run_duration.is_some_and(|run_duration| elapsed >= run_duration) {
        Some("duration reached")
    } else if num_transactions.is_some_and(|budget| txs_scheduled >= budget) {
        Some("requested tx count reached")
    } else {
        None
    }
}

/// Number of transactions to schedule in the next batch, or `None` when the
/// `--num-transactions` budget is exhausted.
fn next_batch_size(
    num_transactions: Option<u64>,
    send_batch_size: usize,
    txs_scheduled: u64,
) -> Option<usize> {
    let Some(budget) = num_transactions else {
        return Some(send_batch_size);
    };
    let remaining = budget.saturating_sub(txs_scheduled);
    // The min() with send_batch_size makes the cast back to usize lossless.
    (remaining > 0).then(|| remaining.min(send_batch_size as u64) as usize)
}

#[allow(clippy::arithmetic_side_effects)]
fn compute_batch_interval(send_batch_size: usize, target_tps: u64) -> Duration {
    let send_batch_size = u128::try_from(send_batch_size).unwrap_or(u128::MAX);
    let target_tps = u128::from(target_tps);
    let batch_interval_nanos = (send_batch_size * 1_000_000_000).div_ceil(target_tps);
    Duration::from_nanos(u64::try_from(batch_interval_nanos).unwrap_or(u64::MAX))
}

fn create_lamports_pool(max_lamports_to_transfer: usize) -> Arc<[u64]> {
    let mut lamports: Vec<u64> = (1..=max_lamports_to_transfer as u64).collect();
    let mut rng = rng();

    lamports.shuffle(&mut rng);
    lamports.into()
}

fn next_lamports_slice(
    lamports_pool: &mut Arc<[u64]>,
    lamports_index: &mut usize,
    total_pairs: usize,
) -> SharedSlice<u64> {
    assert!(total_pairs <= lamports_pool.len());

    if lamports_pool.len().saturating_sub(*lamports_index) < total_pairs {
        *lamports_pool = create_lamports_pool(lamports_pool.len());
        *lamports_index = 0;
    }

    let lamports_end = lamports_index
        .checked_add(total_pairs)
        .expect("lamports slice end must fit usize");
    let lamports = SharedSlice::new(lamports_pool.clone(), *lamports_index..lamports_end);
    *lamports_index = lamports_end;
    lamports
}

pub(crate) fn check_num_conflict_groups(
    num_conflict_groups: Option<NonZeroUsize>,
    num_send_instructions_per_tx: usize,
    generate_tx_batch_size: usize,
) -> Result<(), BenchClientError> {
    let Some(num_conflict_groups) = num_conflict_groups else {
        return Ok(());
    };
    let max_groups = num_send_instructions_per_tx.saturating_mul(generate_tx_batch_size);
    let num_conflict_groups = num_conflict_groups.get();

    if num_conflict_groups > max_groups {
        return Err(BenchClientError::InvalidCliArguments(format!(
            "--num-conflict-groups ({num_conflict_groups}) must be <= \
             num-send-instructions-per-tx ({num_send_instructions_per_tx}) * tx-batch-size \
             ({generate_tx_batch_size})"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::{compute_batch_interval, compute_transfer_tx_min_cu_budget, next_lamports_slice},
        crate::cli::{InstructionPaddingParams, SimpleTransferTxParams, TransactionParams},
        std::{num::NonZeroUsize, sync::Arc},
        tokio::time::Duration,
    };

    #[test]
    fn test_compute_batch_interval() {
        assert_eq!(compute_batch_interval(10, 100), Duration::from_millis(100));
        assert_eq!(compute_batch_interval(1, 1), Duration::from_secs(1));
    }

    #[test]
    fn test_transfer_tx_min_cu_budget_legacy_without_optional_compute_budget_instructions() {
        let transaction_params = make_transaction_params(2, None, false);

        let actual = compute_transfer_tx_min_cu_budget(&transaction_params, None);

        // 1 compute-budget instruction + 2 simple transfer instructions.
        assert_eq!(actual, 150 + 2 * 150);
    }

    #[test]
    fn test_transfer_tx_min_cu_budget_legacy_with_price_and_padding() {
        let transaction_params = make_transaction_params(2, Some(32), false);

        let actual = compute_transfer_tx_min_cu_budget(&transaction_params, Some(1_000));

        // 3 compute-budget instructions (limit + price + loaded-account-size) + 2 padded transfers.
        assert_eq!(actual, 3 * 150 + 2 * 3_000);
    }

    #[test]
    fn test_transfer_tx_min_cu_budget_v1_with_price_and_padding() {
        let transaction_params = make_transaction_params(2, Some(32), true);

        let actual = compute_transfer_tx_min_cu_budget(&transaction_params, Some(1_000));

        // V1 configuration does not insert compute-budget instructions into the message.
        assert_eq!(actual, 2 * 3_000);
    }

    #[test]
    fn test_next_lamports_slice_advances_without_reshuffle() {
        let mut pool: Arc<[u64]> = Arc::from([1, 2, 3, 4]);
        let mut index = 0;

        let first = next_lamports_slice(&mut pool, &mut index, 2);
        let second = next_lamports_slice(&mut pool, &mut index, 2);

        assert_eq!(first.as_slice(), &[1, 2]);
        assert_eq!(second.as_slice(), &[3, 4]);
        assert_eq!(index, 4);
    }

    fn make_transaction_params(
        num_send_instructions_per_tx: usize,
        instruction_padding_data_size: Option<u32>,
        use_txv1: bool,
    ) -> TransactionParams {
        TransactionParams {
            simple_transfer_tx_params: SimpleTransferTxParams {
                max_lamports_to_transfer: 513,
                transfer_tx_cu_budget: 600,
                num_send_instructions_per_tx,
                tx_batch_size: NonZeroUsize::new(64).unwrap(),
                num_conflict_groups: None,
            },
            padding_params: InstructionPaddingParams {
                instruction_padding_data_size,
                instruction_padding_program_id: None,
            },
            use_txv1,
        }
    }
}
