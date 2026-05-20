//! Structure used for serializing and deserializing created accounts.
use {
    crate::accounts_creator::{AccountsCreator, Error as AccountsCreatorError},
    log::error,
    serde::{Deserialize, Serialize},
    solana_keypair::Keypair,
    solana_native_token::LAMPORTS_PER_SOL,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_rpc_client_api::client_error::Error as RpcClientError,
    solana_signer::Signer,
    std::{fs::File, io::Write, path::PathBuf, sync::Arc},
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    AccountsCreatorError(#[from] AccountsCreatorError),

    #[error("Failed to read keypair file")]
    KeypairReadFailure,

    #[error("Accounts validation failed")]
    AccountsValidationFailure,

    #[error("Could not find validator identity among staked nodes")]
    FindValidatorIdentityFailure,

    #[error(transparent)]
    RpcClientError(#[from] RpcClientError),
}

#[derive(Default, Debug, PartialEq)]
pub struct AccountsFile {
    /// Accounts used to pay for transactions.
    /// Many are used to avoid introducing dependencies between transactions.
    pub payers: Vec<Keypair>,
}

pub fn read_accounts_file(path: PathBuf) -> AccountsFile {
    let file_content = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!("Failed to read the accounts file.\nPath: {path:?}\nError: {err}")
    });
    serde_json::from_str::<AccountsFileRaw>(&file_content)
        .unwrap_or_else(|err| {
            panic!(
                "Failed to parse accounts file.\nPath: {path:?}\nError: \
                 {err}\nContent:\n{file_content}"
            )
        })
        .into()
}

pub fn write_accounts_file(path: PathBuf, accounts: AccountsFile) {
    let accounts_file_raw: AccountsFileRaw = accounts.into();
    let file_content = serde_json::to_string(&accounts_file_raw)
        .unwrap_or_else(|err| panic!("Failed to serialize the accounts file.\nError: {err}"));
    let mut file = File::create(path.clone())
        .unwrap_or_else(|err| panic!("Failed to create a file.\nPath: {path:?}\nError: {err}"));

    file.write_all(file_content.as_bytes())
        .unwrap_or_else(|err| {
            panic!(
                "Failed to write the accounts file.\nPath: {path:?}\nError: \
                 {err}\nContent:\n{file_content}"
            )
        });
}

pub async fn create_ephemeral_accounts(
    rpc_client: Arc<RpcClient>,
    authority: Keypair,
    num_payers: usize,
    payers_account_balance_lamports: u64,
    validate_accounts: bool,
) -> Result<AccountsFile, Error> {
    let accounts_creator = AccountsCreator::new(
        rpc_client.clone(),
        authority,
        num_payers,
        payers_account_balance_lamports,
    );
    let accounts = accounts_creator.create().await?;
    if validate_accounts
        && !validate_payers(
            &accounts,
            rpc_client,
            num_payers,
            payers_account_balance_lamports,
        )
        .await?
    {
        return Err(Error::AccountsValidationFailure);
    }

    Ok(accounts)
}

pub async fn create_file_persisted_accounts(
    rpc_client: Arc<RpcClient>,
    authority: Keypair,
    accounts_file: PathBuf,
    num_payers: usize,
    payers_account_balance: u64,
    validate_accounts: bool,
) -> Result<(), Error> {
    let accounts = create_ephemeral_accounts(
        rpc_client,
        authority,
        num_payers,
        payers_account_balance,
        validate_accounts,
    )
    .await?;

    write_accounts_file(accounts_file, accounts);

    Ok(())
}

async fn validate_payers(
    AccountsFile { payers, .. }: &AccountsFile,
    rpc_client: Arc<RpcClient>,
    desired_num: usize,
    desired_balance_lamports: u64,
) -> Result<bool, Error> {
    if payers.len() < desired_num {
        error!(
            "Insufficient number of payers {}, while expected {}",
            payers.len(),
            desired_num
        );
        return Ok(false);
    }
    for payer in payers {
        let balance_sol: u64 = rpc_client.get_balance(&payer.pubkey()).await?;
        if balance_sol.saturating_mul(LAMPORTS_PER_SOL) < desired_balance_lamports {
            error!(
                "Insufficient balance {}SOL for account {}.",
                balance_sol,
                payer.pubkey()
            );
            return Ok(false);
        }
    }
    Ok(true)
}

impl From<AccountsFileRaw> for AccountsFile {
    fn from(AccountsFileRaw { payers }: AccountsFileRaw) -> Self {
        let payers = payers.into_iter().map(Into::into).collect();
        Self { payers }
    }
}

#[derive(Deserialize, Serialize)]
struct AccountsFileRaw {
    #[serde(default)]
    payers: Vec<KeypairRaw>,
}

impl From<AccountsFile> for AccountsFileRaw {
    fn from(AccountsFile { payers }: AccountsFile) -> Self {
        AccountsFileRaw {
            payers: payers.iter().map(KeypairRaw::from).collect(),
        }
    }
}

#[derive(Deserialize, Serialize)]
struct KeypairRaw {
    #[serde(rename = "publicKey")]
    pub _pubkey: String,
    #[serde(rename = "secretKey")]
    pub secret_key: Vec<u8>,
}

impl From<KeypairRaw> for Keypair {
    fn from(raw: KeypairRaw) -> Self {
        assert_eq!(raw.secret_key.len(), 64);
        Self::new_from_array(raw.secret_key[..32].try_into().unwrap())
    }
}

impl From<&Keypair> for KeypairRaw {
    fn from(keypair: &Keypair) -> Self {
        Self {
            _pubkey: keypair.pubkey().to_string(),
            secret_key: keypair.to_bytes().to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_signer::Signer};

    #[test]
    fn test_deserialize_full_accounts_file() {
        let keypair = Keypair::new();
        let pubkey = keypair.pubkey().to_string();
        let secretkey: Vec<u8> = keypair.to_bytes().to_vec();
        let json_data = serde_json::json!({
            "payers": [
                {
                    "publicKey": pubkey,
                    "secretKey": secretkey,
                },
                {
                    "publicKey": pubkey,
                    "secretKey": secretkey,
                },
            ],
        })
        .to_string();

        let accounts = serde_json::from_str::<AccountsFileRaw>(&json_data)
            .expect("The test json should be properly formatted.");
        let actual = AccountsFile::from(accounts);
        assert_eq!(
            actual,
            AccountsFile {
                payers: vec![keypair.insecure_clone(), keypair.insecure_clone()],
            }
        );
    }

    #[test]
    fn test_serialize_full_accounts_file() {
        let keypair = Keypair::new();
        let pubkey = keypair.pubkey().to_string();
        let secretkey = format!("{:?}", keypair.to_bytes());
        let expected = AccountsFile {
            payers: vec![keypair.insecure_clone(), keypair.insecure_clone()],
        };

        let accounts_file_raw: AccountsFileRaw = expected.into();
        let actual_json_data = serde_json::to_string(&accounts_file_raw).unwrap();

        // we cannot use json! macro because it doesn't guarantee the order of fields.
        let mut json_data = format!(
            r#"
        {{
            "payers": [
                {{
                    "publicKey": "{pubkey}",
                    "secretKey": {secretkey}
                }},
                {{
                    "publicKey": "{pubkey}",
                    "secretKey": {secretkey}
                }}
            ]
        }}
        "#
        );
        json_data.retain(|c| !c.is_ascii_whitespace());

        assert_eq!(actual_json_data, json_data);
    }
}
