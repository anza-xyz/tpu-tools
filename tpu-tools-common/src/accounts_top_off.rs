//! Restore persisted payer balances using confirmed RPC transfers.
use {
    crate::accounts_file::read_accounts_file,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_rpc_client_api::{
        client_error::Error as ClientError, response::transaction::Transaction,
    },
    solana_signer::Signer,
    solana_system_interface::instruction::transfer,
    std::{collections::HashSet, path::PathBuf},
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum Error {
    /// An RPC request or transaction failed.
    #[error(transparent)]
    Rpc(#[from] ClientError),
    /// The funding account cannot also be a target account.
    #[error("Authority must not appear in the payer accounts file")]
    AuthorityIsPayer,
}

/// Bring each payer up to `target`, optionally transferring excess to `authority`.
/// The authority pays all fees. Run while the payers are idle. On an uncertain
/// confirmation error this stops without issuing a replacement transaction.
pub async fn top_off_accounts(
    rpc: &RpcClient,
    authority: &Keypair,
    accounts_file: PathBuf,
    target: u64,
    balance_excess: bool,
) -> Result<(), Error> {
    let accounts = read_accounts_file(accounts_file);
    if accounts
        .payers
        .iter()
        .any(|payer| payer.pubkey() == authority.pubkey())
    {
        return Err(Error::AuthorityIsPayer);
    }
    let mut seen = HashSet::new();
    for payer in accounts.payers {
        if !seen.insert(payer.pubkey()) {
            continue;
        }
        let balance = rpc.get_balance(&payer.pubkey()).await?;
        let Some((collect, amount)) = adjustment(balance, target, balance_excess) else {
            continue;
        };
        let blockhash = rpc.get_latest_blockhash().await?;
        let tx = transfer_transaction(authority, &payer, collect, amount, blockhash);
        rpc.send_and_confirm_transaction(&tx).await?;
        log::info!("Updated payer {} to {target} lamports", payer.pubkey());
    }
    Ok(())
}

fn transfer_transaction(
    authority: &Keypair,
    payer: &Keypair,
    collect: bool,
    amount: u64,
    blockhash: Hash,
) -> Transaction {
    let (source, recipient) = if collect {
        (payer, authority.pubkey())
    } else {
        (authority, payer.pubkey())
    };
    let mut signers = vec![authority];
    if collect {
        signers.push(payer);
    }
    Transaction::new_signed_with_payer(
        &[transfer(&source.pubkey(), &recipient, amount)],
        Some(&authority.pubkey()),
        &signers,
        blockhash,
    )
}

fn adjustment(balance: u64, target: u64, balance_excess: bool) -> Option<(bool, u64)> {
    if balance < target {
        Some((false, target.saturating_sub(balance)))
    } else if balance_excess && balance > target {
        Some((true, balance.saturating_sub(target)))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfers_have_correct_signers_and_fee_payer() {
        let authority = Keypair::new();
        let payer = Keypair::new();
        for collect in [false, true] {
            let tx = transfer_transaction(&authority, &payer, collect, 123, Hash::new_unique());
            let message = tx.message_data();
            assert_eq!(tx.signatures[0], authority.sign_message(&message));
            if collect {
                assert_eq!(tx.signatures[1], payer.sign_message(&message));
            }
            assert_eq!(tx.message.account_keys[0], authority.pubkey());
            assert_eq!(tx.signatures.len(), if collect { 2 } else { 1 });
            let ix = &tx.message.instructions[0];
            let source = tx.message.account_keys[usize::from(ix.accounts[0])];
            let destination = tx.message.account_keys[usize::from(ix.accounts[1])];
            assert_eq!(
                source,
                if collect {
                    payer.pubkey()
                } else {
                    authority.pubkey()
                }
            );
            assert_eq!(
                destination,
                if collect {
                    authority.pubkey()
                } else {
                    payer.pubkey()
                }
            );
            assert_eq!(ix.data, transfer(&source, &destination, 123).data);
        }
    }

    #[test]
    fn balance_adjustments() {
        assert_eq!(adjustment(0, 100, false), Some((false, 100)));
        assert_eq!(adjustment(25, 100, false), Some((false, 75)));
        assert_eq!(adjustment(100, 100, true), None);
        assert_eq!(adjustment(150, 100, false), None);
        assert_eq!(adjustment(150, 100, true), Some((true, 50)));
        assert_eq!(adjustment(u64::MAX, 0, true), Some((true, u64::MAX)));
    }
}
