use {
    crate::{
        cli::SimpleTransferTxParams, generator::transaction_builder::create_serialized_transfers,
    },
    log::debug,
    rand::{seq::IteratorRandom, thread_rng},
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_measure::measure::Measure,
    std::sync::Arc,
    tokio::task::JoinHandle,
};

// Generates a transaction batch of simple lamport transfer transactions.
#[allow(clippy::arithmetic_side_effects)]
pub(crate) fn generate_transfer_transaction_batch(
    payers: Arc<Vec<Keypair>>,
    payer_index: usize,
    blockhash: Hash,
    SimpleTransferTxParams {
        lamports_to_transfer,
        transfer_tx_cu_budget,
        num_send_instructions_per_tx,
    }: SimpleTransferTxParams,
    send_batch_size: usize,
) -> JoinHandle<Vec<Vec<u8>>> {
    spawn_blocking_transaction_batch_generation("generate transfer transaction batch", move || {
        let mut txs: Vec<Vec<u8>> = Vec::with_capacity(send_batch_size);
        let lamports_to_transfer = unique_random_numbers(
            num_send_instructions_per_tx * send_batch_size,
            lamports_to_transfer,
        );
        let mut payer = payers.iter().cycle().skip(payer_index);
        let mut lamports = lamports_to_transfer.iter();
        let mut instructions = Vec::with_capacity(num_send_instructions_per_tx);
        let mut signers: Vec<&Keypair> = Vec::with_capacity(num_send_instructions_per_tx);
        for _ in 0..send_batch_size {
            let tx = create_serialized_transfers(
                &mut payer,
                &mut lamports,
                blockhash,
                &mut instructions,
                &mut signers,
                num_send_instructions_per_tx,
                transfer_tx_cu_budget,
            );
            txs.push(tx);
            instructions.clear();
            signers.clear();
        }
        txs
    })
}

fn unique_random_numbers(count: usize, lamports_to_transfer: u64) -> Vec<u64> {
    assert!(
        count as u64 <= lamports_to_transfer,
        "Not enough unique values in range: {count} > {lamports_to_transfer}"
    );

    let mut rng = thread_rng();

    // Sample `count` unique values from the full range
    (1..=lamports_to_transfer).choose_multiple(&mut rng, count)
}

/// Helper to spawn a blocking task for generating a batch of transactions.
/// Manages performance measurement and logging.
fn spawn_blocking_transaction_batch_generation<F>(
    batch_description: &'static str,
    generation_logic: F,
) -> JoinHandle<Vec<Vec<u8>>>
where
    F: FnOnce() -> Vec<Vec<u8>> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let mut measure_generate = Measure::start(batch_description);
        let txs = generation_logic();
        measure_generate.stop();
        debug!(
            "Time to {}: {} us, num transactions in batch: {}",
            batch_description,
            measure_generate.as_us(),
            txs.len(),
        );
        txs
    })
}
