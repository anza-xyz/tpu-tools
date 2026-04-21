use {
    crate::{cli::TransactionParams, generator::transaction_builder::create_serialized_transfers},
    log::debug,
    rand::{seq::IteratorRandom, thread_rng},
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_measure::measure::Measure,
    std::{num::NonZeroUsize, sync::Arc},
    tokio::task::JoinHandle,
};

// Generates a transaction batch of simple lamport transfer transactions.
#[allow(clippy::arithmetic_side_effects)]
pub(crate) fn generate_transfer_transaction_batch(
    payers: Arc<Vec<Keypair>>,
    payer_index: usize,
    blockhash: Hash,
    TransactionParams {
        simple_transfer_tx_params,
        padding_params,
    }: TransactionParams,
    send_batch_size: usize,
    compute_unit_price: u64,
    random_compute_unit_price_max: u64,
) -> JoinHandle<Vec<Vec<u8>>> {
    spawn_blocking_transaction_batch_generation("generate transfer transaction batch", move || {
        let mut txs: Vec<Vec<u8>> = Vec::with_capacity(send_batch_size);
        let instruction_padding_config = TransactionParams {
            simple_transfer_tx_params: simple_transfer_tx_params.clone(),
            padding_params: padding_params.clone(),
        }
        .instruction_padding_config();

        let lamports_to_transfer = simple_transfer_tx_params.lamports_to_transfer;
        let transfer_tx_cu_budget = simple_transfer_tx_params.transfer_tx_cu_budget;
        let num_send_instructions_per_tx = simple_transfer_tx_params.num_send_instructions_per_tx;
        let num_conflict_groups = simple_transfer_tx_params.num_conflict_groups;

        let total_pairs = num_send_instructions_per_tx * send_batch_size;

        let lamports_to_transfer = unique_random_numbers(total_pairs, lamports_to_transfer);
        let (accounts_from, accounts_to) =
            build_accounts_from_to_lists(&payers, payer_index, total_pairs, num_conflict_groups);

        let mut accounts_from_iter = accounts_from.iter().copied();
        let mut accounts_to_iter = accounts_to.iter().copied();
        let mut lamports = lamports_to_transfer.iter();
        let mut instructions = Vec::with_capacity(num_send_instructions_per_tx);
        let mut signers: Vec<&Keypair> = Vec::with_capacity(num_send_instructions_per_tx);

        for _ in 0..send_batch_size {
            let tx = create_serialized_transfers(
                &mut accounts_from_iter,
                &mut accounts_to_iter,
                &mut lamports,
                blockhash,
                &mut instructions,
                &mut signers,
                num_send_instructions_per_tx,
                transfer_tx_cu_budget,
                instruction_padding_config.as_ref(),
                compute_unit_price,
                random_compute_unit_price_max,
            );
            txs.push(tx);
            instructions.clear();
            signers.clear();
        }
        txs
    })
}

/// Build separate accounts-from and accounts-to lists from the flat payers pool.
#[allow(clippy::arithmetic_side_effects)]
fn build_accounts_from_to_lists<'a>(
    payers: &'a [Keypair],
    payer_index: usize,
    total_pairs: usize,
    num_conflict_groups: Option<NonZeroUsize>,
) -> (Vec<&'a Keypair>, Vec<&'a Keypair>) {
    let len = payers.len();

    // Collect accounts-from: send_batch_size keypairs starting at payer_index
    let accounts_from: Vec<&'a Keypair> = payers
        .iter()
        .cycle()
        .skip(payer_index % len)
        .take(total_pairs)
        .collect();

    let receiver_start = payer_index + total_pairs;
    let receiver_offset = receiver_start % len;

    // Collect accounts-to: send_batch_size keypairs starting after accounts-from
    let accounts_to: Vec<&'a Keypair> = match num_conflict_groups {
        None => payers
            .iter()
            .cycle()
            .skip(receiver_offset)
            .take(total_pairs)
            .collect(),
        Some(n) => {
            let pool_len = n.get();
            debug_assert!(pool_len <= total_pairs);

            let pool_iter = payers.iter().cycle().skip(receiver_offset).take(pool_len);

            pool_iter.cycle().take(total_pairs).collect()
        }
    };

    (accounts_from, accounts_to)
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

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_keypair::{Keypair, Signer},
    };

    #[test]
    fn test_no_conflict_groups_all_receivers_unique() {
        let keypairs: Vec<Keypair> = (0..16).map(|_| Keypair::new()).collect();
        let (accounts_from, accounts_to) = build_accounts_from_to_lists(&keypairs, 0, 4, None);

        // All 4 accounts-from are unique
        assert_eq!(accounts_from.len(), 4);
        let sender_pubkeys: Vec<_> = accounts_from.iter().map(|k| k.pubkey()).collect();
        assert_eq!(
            sender_pubkeys.len(),
            sender_pubkeys
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
        );

        // All 4 accounts-to are unique
        assert_eq!(accounts_to.len(), 4);
        let receiver_pubkeys: Vec<_> = accounts_to.iter().map(|k| k.pubkey()).collect();
        assert_eq!(
            receiver_pubkeys.len(),
            receiver_pubkeys
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
        );
    }

    #[test]
    fn test_conflict_groups_receivers_repeat() {
        let keypairs: Vec<Keypair> = (0..16).map(|_| Keypair::new()).collect();
        let (accounts_from, accounts_to) =
            build_accounts_from_to_lists(&keypairs, 0, 4, Some(NonZeroUsize::new(2).unwrap()));

        // 4 accounts-from, all unique
        assert_eq!(accounts_from.len(), 4);

        // 4 accounts-to slots but only 2 unique pubkeys
        assert_eq!(accounts_to.len(), 4);
        assert_eq!(accounts_to[0].pubkey(), accounts_to[2].pubkey());
        assert_eq!(accounts_to[1].pubkey(), accounts_to[3].pubkey());
        assert_ne!(accounts_to[0].pubkey(), accounts_to[1].pubkey());
    }

    #[test]
    fn test_conflict_groups_one_all_same_receiver() {
        let keypairs: Vec<Keypair> = (0..16).map(|_| Keypair::new()).collect();
        let (_accounts_from, accounts_to) =
            build_accounts_from_to_lists(&keypairs, 0, 4, Some(NonZeroUsize::new(1).unwrap()));

        // All 4 accounts-to are the same account
        assert_eq!(accounts_to[0].pubkey(), accounts_to[1].pubkey());
        assert_eq!(accounts_to[1].pubkey(), accounts_to[2].pubkey());
        assert_eq!(accounts_to[2].pubkey(), accounts_to[3].pubkey());
    }
}
