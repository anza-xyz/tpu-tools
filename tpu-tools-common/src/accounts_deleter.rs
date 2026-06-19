//! RPC-backed payer account draining for TPU tools.
//!
//! This module reads payer keypairs from an account file, fetches their current
//! balances, and transfers those lamports to a recipient account.
#![allow(clippy::arithmetic_side_effects)]
use {
    crate::{accounts_file::read_accounts_file, blockhash_updater::BlockhashUpdater},
    futures::future::join_all,
    log::*,
    solana_hash::Hash,
    solana_instruction::Instruction,
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_rpc_client_api::{
        client_error::Error as ClientError,
        response::transaction::{Transaction, versioned::VersionedTransaction},
    },
    solana_signer::Signer,
    solana_system_interface::instruction as system_instruction,
    std::{collections::HashSet, path::PathBuf, sync::Arc},
    thiserror::Error,
    tokio::{
        sync::watch,
        time::{Duration, sleep},
    },
};

/// Max transfer instructions packed into one transaction (packet size limit).
const MAX_DELETE_ACC_IX_PER_TX: usize = 9;
/// Used to sleep between account draining attempts to avoid getting 429s from RPC.
const ACCOUNT_DELETION_SLEEP_INTERVAL: Duration = Duration::from_millis(150);
/// Max number of unsuccessful delete accounts attempts.
const MAX_CONTINUOUS_FAILED_ATTEMPTS: usize = 100;

#[derive(Error, Debug)]
pub enum Error {
    /// RPC client request failed.
    #[error(transparent)]
    ClientError(#[from] ClientError),

    /// Account draining did not drain every funded account.
    #[error("Failed to delete account")]
    DeleteAccountFailure,
}

/// Reads payer accounts from a file and drains their lamports to `recipient`.
///
/// The `authority` keypair pays transaction fees, allowing each payer account
/// to transfer its full current balance.
pub async fn delete_file_persisted_accounts(
    rpc_client: Arc<RpcClient>,
    authority: Keypair,
    accounts_file: PathBuf,
    recipient: Pubkey,
    txn_batch_size: usize,
) -> Result<(), Error> {
    let accounts = read_accounts_file(accounts_file);
    let account_balances = fetch_account_balances(&rpc_client, accounts.payers).await?;
    let num_accounts_with_balance = account_balances.len();

    if num_accounts_with_balance == 0 {
        info!("No funded payer accounts to delete.");
        return Ok(());
    }

    let deleted_accounts = delete_accounts(
        &rpc_client,
        &authority,
        &recipient,
        account_balances,
        txn_batch_size,
        MAX_CONTINUOUS_FAILED_ATTEMPTS,
    )
    .await;

    if deleted_accounts != num_accounts_with_balance {
        error!(
            "Failed to delete all funded payers: {deleted_accounts}/{num_accounts_with_balance} \
             deleted"
        );
        return Err(Error::DeleteAccountFailure);
    }

    info!("Payers have been deleted.");
    Ok(())
}

async fn fetch_account_balances(
    rpc_client: &Arc<RpcClient>,
    payers: Vec<Keypair>,
) -> Result<Vec<(Keypair, u64)>, Error> {
    let balance_futures = payers.iter().map(|payer| {
        let pubkey = payer.pubkey();
        async move { rpc_client.get_balance(&pubkey).await }
    });
    let balances = join_all(balance_futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    Ok(payers
        .into_iter()
        .zip(balances)
        .filter(|(_, balance)| *balance > 0)
        .collect())
}

fn create_delete_transaction_batch(
    authority: &Keypair,
    recipient: &Pubkey,
    blockhash: Hash,
    account_balances: &[(Keypair, u64)],
) -> Vec<(VersionedTransaction, Vec<Pubkey>)> {
    let mut remaining_accounts = account_balances;
    let mut transactions = Vec::new();

    while !remaining_accounts.is_empty() {
        let chunk_size = remaining_accounts.len().min(MAX_DELETE_ACC_IX_PER_TX);
        let (chunk, remaining) = remaining_accounts.split_at(chunk_size);
        remaining_accounts = remaining;

        let instructions: Vec<Instruction> = chunk
            .iter()
            .map(|(payer, balance)| {
                system_instruction::transfer(&payer.pubkey(), recipient, *balance)
            })
            .collect();
        let signers: Vec<&Keypair> = std::iter::once(authority)
            .chain(chunk.iter().map(|(payer, _)| payer))
            .collect();
        let drained_accounts = chunk.iter().map(|(payer, _)| payer.pubkey()).collect();
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&authority.pubkey()),
            &signers,
            blockhash,
        )
        .into();

        transactions.push((transaction, drained_accounts));
    }

    transactions
}

async fn send_transaction_batch(
    rpc_client: &Arc<RpcClient>,
    transaction_batch: Vec<(VersionedTransaction, Vec<Pubkey>)>,
) -> Vec<Pubkey> {
    let futures = transaction_batch
        .into_iter()
        .map(|(tx, account_pubkeys)| async move {
            (
                rpc_client.send_and_confirm_transaction(&tx).await,
                account_pubkeys,
            )
        });
    let results = join_all(futures).await;
    results
        .into_iter()
        .filter_map(|(result, account_pubkeys)| result.ok().map(|_| account_pubkeys))
        .flatten()
        .collect()
}

fn calculate_batch_size(
    num_accounts: usize,
    num_deleted_accounts: usize,
    num_send_batch_attempts: usize,
    txn_batch_size: usize,
) -> usize {
    let mean_num_success = num_deleted_accounts
        .checked_div(num_send_batch_attempts)
        .unwrap_or(std::cmp::min(num_accounts, txn_batch_size));
    std::cmp::min(mean_num_success + 1, num_accounts - num_deleted_accounts)
}

async fn delete_accounts(
    rpc_client: &Arc<RpcClient>,
    authority: &Keypair,
    recipient: &Pubkey,
    account_balances: Vec<(Keypair, u64)>,
    txn_batch_size: usize,
    max_continuous_failed_attempts: usize,
) -> usize {
    let num_accounts = account_balances.len();
    let mut pending_accounts = account_balances;
    let mut num_send_batch_attempts = 0;
    let mut num_continuous_failed_attempts = 0;

    let blockhash = loop {
        if num_continuous_failed_attempts >= max_continuous_failed_attempts {
            return 0;
        }

        if let Ok(bh) = rpc_client.get_latest_blockhash().await {
            break bh;
        }
        num_continuous_failed_attempts += 1;
        sleep(ACCOUNT_DELETION_SLEEP_INTERVAL).await;
    };

    let (blockhash_sender, blockhash_receiver) = watch::channel(blockhash);
    let blockhash_updater = BlockhashUpdater::new(rpc_client.clone(), blockhash_sender);

    tokio::spawn(async move { blockhash_updater.run().await });

    while !pending_accounts.is_empty() {
        let num_deleted_accounts = num_accounts - pending_accounts.len();
        if num_continuous_failed_attempts >= max_continuous_failed_attempts {
            error!(
                "Failed to delete accounts. num_send_batch_attempts: {num_send_batch_attempts}, \
                 num_deleted_accounts: {num_deleted_accounts}."
            );
            break;
        }

        let blockhash = *blockhash_receiver.borrow();
        let current_batch_size = calculate_batch_size(
            num_accounts,
            num_deleted_accounts,
            num_send_batch_attempts,
            txn_batch_size,
        );
        debug!(
            "current_batch_size: {current_batch_size}, num_deleted_accounts: \
             {num_deleted_accounts}, num_continuous_failed_attempts: \
             {num_continuous_failed_attempts}."
        );

        let transaction_batch = create_delete_transaction_batch(
            authority,
            recipient,
            blockhash,
            &pending_accounts[..current_batch_size],
        );
        let deleted_accounts = send_transaction_batch(rpc_client, transaction_batch).await;
        num_continuous_failed_attempts = if deleted_accounts.is_empty() {
            num_continuous_failed_attempts + 1
        } else {
            0
        };

        let deleted_accounts: HashSet<_> = deleted_accounts.into_iter().collect();
        pending_accounts.retain(|(payer, _)| !deleted_accounts.contains(&payer.pubkey()));

        num_send_batch_attempts += 1;
        sleep(ACCOUNT_DELETION_SLEEP_INTERVAL).await;
    }

    num_accounts - pending_accounts.len()
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_keypair::Keypair, solana_rpc_client::nonblocking::rpc_client::RpcClient,
    };

    #[tokio::test]
    async fn test_delete_transaction_size_within_txn_limit() {
        let rpc = RpcClient::new_mock("succeeds".to_string());
        let blockhash = rpc.get_latest_blockhash().await.unwrap();
        let authority = Keypair::new();
        let recipient = Pubkey::new_unique();
        let account_balances: Vec<_> = (0..MAX_DELETE_ACC_IX_PER_TX)
            .map(|_| (Keypair::new(), 1_000))
            .collect();

        let txs =
            create_delete_transaction_batch(&authority, &recipient, blockhash, &account_balances);

        assert_eq!(txs.len(), 1);

        const SOLANA_TXN_MAX_BYTES: usize = 1232;
        let txn_size = bincode::serialized_size(&txs[0].0)
            .expect("transaction should be bincode-serializable") as usize;
        assert!(
            txn_size <= SOLANA_TXN_MAX_BYTES,
            "serialized transaction size {txn_size} exceeds Solana limit {SOLANA_TXN_MAX_BYTES}"
        );
    }

    #[test]
    fn test_zero_balance_accounts_are_skipped() {
        let funded = Keypair::new();
        let zero_balance = Keypair::new();
        let account_balances = vec![(funded.insecure_clone(), 100), (zero_balance, 0)];

        let filtered: Vec<_> = account_balances
            .into_iter()
            .filter(|(_, balance)| *balance > 0)
            .collect();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0.pubkey(), funded.pubkey());
    }
}
