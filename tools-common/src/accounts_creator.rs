//! Create accounts which are later employed to create transactions.
//! Using RpcClient for simplicity.
#![allow(clippy::arithmetic_side_effects)]
use {
    crate::accounts_file::{write_accounts_file, AccountsFile},
    chrono::prelude::Utc,
    futures::future::join_all,
    log::*,
    solana_clock::DEFAULT_MS_PER_SLOT,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_message::Message,
    solana_pubkey::Pubkey,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_rpc_client_api::client_error::Error as ClientError,
    solana_sdk_ids::system_program,
    solana_signer::Signer,
    solana_system_interface::instruction as system_instruction,
    solana_transaction::Transaction,
    std::{path::PathBuf, sync::Arc},
    thiserror::Error,
    tokio::time::{sleep, Duration},
};

/// How many transactions send concurrently.
const MAX_RPC_SEND_TX_BATCH: usize = 64;
/// Used to sleep between accounts creation to avoid getting 429s from RPC.
const ACCOUNT_CREATION_SLEEP_INTERVAL: Duration = Duration::from_millis(150);

/// Max number of unsuccessful create accounts attempts.
/// The total time waiting for successful account creation is
/// `MAX_CONTINUOUS_FAILED_ATTEMPTS*ACCOUNT_CREATION_SLEEP_INTERVAL`
const MAX_CONTINUOUS_FAILED_ATTEMPTS: usize = 100;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    ClientError(#[from] ClientError),

    #[error("Failed to airdrop")]
    AirdropFailure,

    #[error("Failed to create account")]
    CreateAccountFailure,
}

pub struct AccountsCreator {
    rpc_client: Arc<RpcClient>,
    authority: Keypair,
    num_payers: usize,
    payer_account_balance_lamports: u64,
}

impl AccountsCreator {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        authority: Keypair,
        num_payers: usize,
        payer_account_balance_lamports: u64,
    ) -> Self {
        Self {
            rpc_client,
            authority,
            num_payers,
            payer_account_balance_lamports,
        }
    }

    pub async fn create(&self) -> Result<AccountsFile, Error> {
        self.ensure_authority_balance().await?;
        let payers = self.create_payers().await;

        if payers.len() != self.num_payers {
            error!(
                "Failed to create all payers: {}/{} created",
                payers.len(),
                self.num_payers
            );
            if !payers.is_empty() {
                save_partial_results(payers);
            }
            return Err(Error::CreateAccountFailure);
        }

        info!("Payers have been created.");

        Ok(AccountsFile { payers })
    }

    async fn ensure_authority_balance(&self) -> Result<(), Error> {
        let authority_pubkey = self.authority.pubkey();
        let rpc_client = &*self.rpc_client;

        // Compute the minimum budget for payers
        let min_balance_to_create_account =
            self.request_create_account_tx_fee(0).await? + self.payer_account_balance_lamports;
        let required_balance = self.num_payers as u64 * min_balance_to_create_account;
        let actual_balance = rpc_client.get_balance(&authority_pubkey).await?;
        info!("Authority balance {actual_balance}, min required balance {required_balance}");

        if actual_balance >= required_balance {
            return Ok(());
        }

        // The authority needs more SOL.
        let balance_shortage = required_balance.saturating_sub(actual_balance);
        rpc_client
            .request_airdrop(&authority_pubkey, balance_shortage)
            .await?;

        // TODO(klykov): Wait for two blocks before checking.
        // Maybe it is better to check the status using the returned signature?
        sleep(Duration::from_millis(2 * DEFAULT_MS_PER_SLOT)).await;

        let actual_balance = rpc_client.get_balance(&authority_pubkey).await?;
        info!("Balance after airdrop {actual_balance}");

        if actual_balance < required_balance {
            return Err(Error::AirdropFailure);
        }

        Ok(())
    }

    /// Computes the fee to create account of given size.
    async fn request_create_account_tx_fee(&self, size: u64) -> Result<u64, Error> {
        // Create dummy create account transaction message to calculate fee
        let rent = self
            .rpc_client
            .get_minimum_balance_for_rent_exemption(size as usize)
            .await?;
        let payer_pubkey = Pubkey::new_unique();
        let instructions = vec![system_instruction::create_account(
            &payer_pubkey,
            &Pubkey::new_unique(),
            rent,
            size,
            &system_program::id(),
        )];

        let blockhash = self.rpc_client.get_latest_blockhash().await?;
        let message = Message::new_with_blockhash(&instructions, Some(&payer_pubkey), &blockhash);
        let fee = self.rpc_client.get_fee_for_message(&message).await?;
        Ok(fee)
    }

    async fn create_payers(&self) -> Vec<Keypair> {
        create_accounts(
            &self.rpc_client,
            &[self.authority.insecure_clone()],
            self.num_payers,
            self.payer_account_balance_lamports,
            MAX_CONTINUOUS_FAILED_ATTEMPTS,
        )
        .await
    }
}

fn save_partial_results(payers: Vec<Keypair>) {
    let timestamp = Utc::now().format("%Y-%m-%dT%H-%M-%S").to_string();

    let file_name = format!("accounts-dump-{timestamp}.json");
    let mut path = PathBuf::from("./");
    path.push(file_name);
    info!("Save partial results to file: {path:?}.");
    let accounts = AccountsFile { payers };
    write_accounts_file(path, accounts);
}

fn create_transaction_batch(
    authorities: &[Keypair],
    blockhash: Hash,
    current_batch_size: usize,
    balance_lamports: u64,
) -> Vec<(Transaction, Keypair)> {
    let mut authorities_iter = authorities.iter().cycle();
    (0..current_batch_size)
        .map(|_| {
            let new_account = Keypair::new();
            let authority = authorities_iter
                .next()
                .expect("Authorities slice should not be empty because it is cyclical.");
            let instructions = vec![system_instruction::create_account(
                &authority.pubkey(),
                &new_account.pubkey(),
                balance_lamports,
                0,
                &system_program::id(),
            )];

            let message = Message::new(&instructions, Some(&authority.pubkey()));
            (
                Transaction::new(&[authority, &new_account], message, blockhash),
                new_account,
            )
        })
        .collect()
}

async fn send_transaction_batch(
    rpc_client: &Arc<RpcClient>,
    transaction_batch: Vec<(Transaction, Keypair)>,
) -> Vec<Keypair> {
    // send txs concurrently to RPC with confirmation
    let futures = transaction_batch
        .into_iter()
        .map(|(tx, account_keypair)| async move {
            (
                rpc_client.send_and_confirm_transaction(&tx).await,
                account_keypair,
            )
        });
    // check how many landed
    let results = join_all(futures).await;
    results
        .into_iter()
        .filter_map(|(result, account_keypair)| result.ok().map(|_| account_keypair))
        .collect()
}

/// Calculate the batch_size dynamically.
/// Assuming rps is more or less constant, batch_size will converge to the mean rps.
fn calculate_batch_size(
    num_accounts: usize,
    num_created_accounts: usize,
    num_send_batch_attempts: usize,
) -> usize {
    let mean_num_success = if num_send_batch_attempts == 0 {
        std::cmp::min(num_accounts, MAX_RPC_SEND_TX_BATCH)
    } else {
        num_created_accounts / num_send_batch_attempts
    };

    std::cmp::min(mean_num_success + 1, num_accounts - num_created_accounts)
}

/// Create accounts with specified parameters.
/// In case of failure, might return less accounts than requested.
async fn create_accounts(
    rpc_client: &Arc<RpcClient>,
    authorities: &[Keypair],
    num_accounts: usize,
    balance_lamports: u64,
    max_continuos_failed_attempts: usize,
) -> Vec<Keypair> {
    // It makes sense to send concurrently subset
    // of transactions to avoid having expired block height exceed error.
    // Take into account that the total size of allocated memory in
    // the block is limited by MAX_BLOCK_ACCOUNTS_DATA_SIZE_DELTA
    // which is ~100MB on the moment of writing.

    let mut created_accounts = Vec::with_capacity(num_accounts);

    let mut num_send_batch_attempts = 0;
    let mut num_continuous_failed_attempts = 0;
    while created_accounts.len() < num_accounts {
        let num_created_accounts = created_accounts.len();
        if num_continuous_failed_attempts >= max_continuos_failed_attempts {
            error!(
                "Failed to create accounts. num_send_batch_attempts: {num_send_batch_attempts}, \
                 num_created_accounts: {num_created_accounts}.",
            );
            break;
        }

        let current_batch_size =
            calculate_batch_size(num_accounts, num_created_accounts, num_send_batch_attempts);
        debug!(
            "current_batch_size: {current_batch_size}, num_created_accounts: \
             {num_created_accounts}, num_continuous_failed_attempts: \
             {num_continuous_failed_attempts}."
        );

        let blockhash = rpc_client.get_latest_blockhash().await;
        if let Err(error) = blockhash {
            warn!("Failed to fetch blockhash. Error: {error}. Wait and try again.");
            sleep(ACCOUNT_CREATION_SLEEP_INTERVAL).await;
            num_continuous_failed_attempts += 1;
            continue;
        }
        let blockhash = blockhash.unwrap();

        let transaction_batch =
            create_transaction_batch(authorities, blockhash, current_batch_size, balance_lamports);
        let newly_created_accounts = send_transaction_batch(rpc_client, transaction_batch).await;
        num_continuous_failed_attempts = if newly_created_accounts.is_empty() {
            num_continuous_failed_attempts + 1
        } else {
            0
        };
        created_accounts.extend(newly_created_accounts);

        num_send_batch_attempts += 1;
        sleep(ACCOUNT_CREATION_SLEEP_INTERVAL).await;
    }
    created_accounts
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        async_trait::async_trait,
        rand::{rngs::StdRng, Rng, SeedableRng},
        solana_keypair::Keypair,
        solana_rpc_client::{
            mock_sender::MockSender,
            rpc_client::RpcClientConfig,
            rpc_sender::{RpcSender, RpcTransportStats},
        },
        solana_rpc_client_api::request::RpcRequest,
        std::sync::{Arc, Mutex},
    };

    /// RpcSender that randomly pick provided MockSenders to send request.
    /// This allows to model different RPC conditions on testnet.
    struct MultiMockSender {
        mock_senders: Vec<MockSender>,
        rng: Arc<Mutex<StdRng>>,
    }

    impl MultiMockSender {
        fn new(mock_senders: Vec<MockSender>, seed: u64) -> Self {
            let rng = StdRng::seed_from_u64(seed);
            Self {
                mock_senders,
                rng: Arc::new(Mutex::new(rng)),
            }
        }

        fn get_random_index(&self) -> usize {
            let mut rng = self.rng.lock().unwrap();
            rng.gen_range(0..self.mock_senders.len())
        }
    }

    #[async_trait]
    impl RpcSender for MultiMockSender {
        fn get_transport_stats(&self) -> RpcTransportStats {
            RpcTransportStats::default()
        }

        async fn send(
            &self,
            request: RpcRequest,
            params: serde_json::Value,
        ) -> solana_rpc_client_api::client_error::Result<serde_json::Value> {
            let index = self.get_random_index();
            self.mock_senders[index].send(request, params).await
        }

        fn url(&self) -> String {
            let index = self.get_random_index();
            self.mock_senders[index].url()
        }
    }

    fn create_mock_rpc_client(urls: &[&str], seed: u64) -> RpcClient {
        let mock_senders = urls.iter().map(MockSender::new).collect();
        let sender = MultiMockSender::new(mock_senders, seed);
        RpcClient::new_sender(sender, RpcClientConfig::default())
    }

    /// Test that `create_accounts` creates required number of accounts if rpc requests
    /// always succeeds.
    #[tokio::test]
    async fn test_create_accounts_rpc_always_succeeds() {
        let rpc_client = Arc::new(RpcClient::new_mock("succeeds".to_string()));

        let accounts = create_accounts(&rpc_client, &[Keypair::new()], 128, 1, 10).await;

        assert_eq!(accounts.len(), 128);
    }

    /// Test that if rpc always fails, `create_accounts` returns correct error.
    #[tokio::test]
    async fn test_create_accounts_rpc_always_fails() {
        let rpc_client = Arc::new(RpcClient::new_mock("fails".to_string()));

        let accounts = create_accounts(&rpc_client, &[Keypair::new()], 128, 1, 10).await;

        assert_eq!(accounts.len(), 0);
    }

    /// Test that if only send transaction rpc call always fails, `create_accounts` returns correct error.
    /// This is situation modeled with "malicious" mock which returns wrong signature for sendTransaction call,
    /// while other rpc calls are successful.
    #[tokio::test]
    async fn test_create_accounts_rpc_send_fails() {
        let rpc_client = Arc::new(RpcClient::new_mock("malicious".to_string()));

        let accounts = create_accounts(&rpc_client, &[Keypair::new()], 1, 1, 10).await;

        assert_eq!(accounts.len(), 0);
    }

    /// Tests that `create_accounts` can handle RPC errors correctly.
    /// Combines a successful RPC endpoint with endpoint that always fails.
    #[tokio::test]
    async fn test_create_accounts_half_rpc_succeeds() {
        let seed = 12345;
        let rpc_client = Arc::new(create_mock_rpc_client(&["succeeds", "fails"], seed));

        let accounts = create_accounts(&rpc_client, &[Keypair::new()], 12, 1, 10).await;

        assert_eq!(accounts.len(), 12);
    }

    /// Tests that `create_accounts` handles transaction errors correctly.
    /// Combines a successful RPC endpoint with endpoints where the `getSignatureStatuses` RPC call returns different transaction errors.
    #[tokio::test]
    async fn test_create_accounts_transaction_errors() {
        let seed = 12345;
        let rpc_client = Arc::new(create_mock_rpc_client(
            &[
                "succeeds",
                "succeeds",
                "succeeds",
                "account_in_use",
                "instruction_error",
                "sig_not_found",
            ],
            seed,
        ));

        let accounts = create_accounts(&rpc_client, &[Keypair::new()], 121, 1, 10).await;

        assert_eq!(accounts.len(), 121);
    }
}
