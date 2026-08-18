//! Shared command-line argument types and parsers.
//!
//! These types are flattened into the TPU tool CLIs so both tools expose
//! consistent account, URL, duration, and leader-tracking options.

use {
    clap::{Args, Subcommand},
    solana_clap_v3_utils::{
        input_parsers::parse_url_or_moniker, input_validators::normalize_to_url_if_moniker,
    },
    solana_keypair::Keypair,
    solana_native_token::LAMPORTS_PER_SOL,
    solana_pubkey::Pubkey,
    solana_signer::{EncodableKey, Signer},
    std::{net::SocketAddr, path::PathBuf, str::FromStr},
    tokio::time::Duration,
};

const MAX_RPC_SEND_TX_BATCH: usize = 60;

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
#[clap(rename_all = "kebab-case")]
/// Leader tracking mode selected from the command line.
pub enum LeaderTracker {
    /// Send to a fixed TPU address instead of tracking cluster leaders.
    #[clap(
        about = "Use pinned address to send transactions to, which means we are not interested in \
                 leader slot updates."
    )]
    PinnedLeaderTracker { address: SocketAddr },

    /// Use the `solana-tpu-client-next` websocket node-address service.
    #[clap(about = "Use ws for slot updates. WS url is generated from the RPC url.")]
    WsLeaderTracker,

    /// Use Yellowstone gRPC slot updates.
    #[clap(about = "Use yellowstone grpc for slot updates instead of ws.")]
    YellowstoneLeaderTracker {
        /// gRPC endpoint URL (positional argument)
        url: String,
        /// gRPC token (optional)
        token: Option<String>,
    },

    /// Use a custom UDP/Geyser slot updater.
    #[clap(about = "Use custom slot updater geyser plugin which sends slot updates over UDP.")]
    CustomLeaderTracker { bind_address: SocketAddr },
}

/// Common payer account creation parameters.
#[derive(Args, Copy, Clone, Debug, PartialEq, Eq)]
#[clap(rename_all = "kebab-case")]
pub struct AccountParams {
    #[clap(
        long,
        default_value = "8",
        help = "Number of payer accounts, using few of them allows to avoid `AccountInUse` errors."
    )]
    pub num_payers: usize,

    #[clap(
        long,
        default_value = "1SOL",
        value_parser = parse_balance,
        help = "Payer account balance in SOL or LAMPORTS,\n\
                used to fund creation of other accounts and for transactions.\n"
    )]
    pub payer_account_balance: u64,
}

/// Parameters for creating and writing payer accounts to a file.
#[derive(Args, Debug, PartialEq, Eq, Clone)]
#[clap(rename_all = "kebab-case")]
pub struct WriteAccounts {
    #[clap(long, help = "File to save the created accounts into.")]
    pub accounts_file: PathBuf,

    #[clap(flatten)]
    pub account_params: AccountParams,
}

/// Parameters for reading payer accounts from a file.
#[derive(Args, Debug, PartialEq, Eq, Clone)]
#[clap(rename_all = "kebab-case")]
pub struct ReadAccounts {
    #[clap(long, help = "File to read the accounts from.")]
    pub accounts_file: PathBuf,
}

/// Parameters for draining payer accounts from a file.
#[derive(Args, Debug, PartialEq, Eq, Clone)]
#[clap(rename_all = "kebab-case")]
pub struct DeleteAccounts {
    #[clap(long, help = "File to read the accounts from.")]
    pub accounts_file: PathBuf,

    #[clap(
        long,
        help = "Account that receives all drained lamports. Accepts a base58 pubkey or a keypair \
                file path."
    )]
    pub recipient: String,

    #[clap(long, default_value_t = MAX_RPC_SEND_TX_BATCH, help = "Maximum number of transactions to send concurrently in a batch.")]
    pub txn_batch_size: usize,
}

/// Parses a recipient as a base58 pubkey, or reads a pubkey from a keypair file path.
pub fn parse_recipient(recipient: &str) -> Result<Pubkey, String> {
    let recipient = recipient.trim();

    if let Ok(pubkey) = Pubkey::from_str(recipient) {
        return Ok(pubkey);
    }

    let path = PathBuf::from(recipient);
    if let Ok(keypair) = Keypair::read_from_file(&path) {
        return Ok(keypair.pubkey());
    }

    Err(format!(
        "invalid recipient '{recipient}': expected a base58 pubkey or a keypair file path"
    ))
}

/// Parses an RPC URL or Solana moniker and normalizes monikers to URLs.
pub fn parse_and_normalize_url(addr: &str) -> Result<String, String> {
    match parse_url_or_moniker(addr) {
        Ok(parsed) => Ok(normalize_to_url_if_moniker(&parsed)),
        Err(e) => Err(format!("Invalid URL or moniker: {e}")),
    }
}

/// Parses a duration in seconds.
pub fn parse_duration_sec(s: &str) -> Result<Duration, &'static str> {
    s.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|_| "failed to parse duration in seconds")
}

/// Parses a duration in milliseconds.
pub fn parse_duration_ms(s: &str) -> Result<Duration, &'static str> {
    s.parse::<u64>()
        .map(Duration::from_millis)
        .map_err(|_| "failed to parse duration in milliseconds")
}

/// Parses strings like "1SOL", "0.5SOL", "1000000000LAMPORTS" into lamports.
fn parse_balance(s: &str) -> Result<u64, String> {
    let s = s.trim().to_uppercase();

    if let Some(sol_value) = s.strip_suffix("SOL") {
        let sol: f64 = sol_value.parse::<f64>().map_err(|e| e.to_string())?;
        Ok((sol * LAMPORTS_PER_SOL as f64) as u64)
    } else if let Some(lamports_str) = s.strip_suffix("LAMPORTS") {
        lamports_str.parse::<u64>().map_err(|e| e.to_string())
    } else {
        // Default to SOL if no suffix
        let sol: f64 = s.parse::<f64>().map_err(|e| e.to_string())?;
        Ok((sol * LAMPORTS_PER_SOL as f64) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_recipient_from_pubkey() {
        let pubkey = Pubkey::new_unique();
        assert_eq!(parse_recipient(&pubkey.to_string()).unwrap(), pubkey);
    }

    #[test]
    fn test_parse_recipient_rejects_invalid_value() {
        assert!(parse_recipient("not-a-pubkey-or-keypair").is_err());
    }

    #[test]
    fn test_parse_balance() {
        assert_eq!(parse_balance("1SOL").unwrap(), 1_000_000_000);
        assert_eq!(parse_balance("0.5SOL").unwrap(), 500_000_000);
        assert_eq!(parse_balance("2.25SOL").unwrap(), 2_250_000_000);

        assert_eq!(parse_balance(" 3sol ").unwrap(), 3_000_000_000);
        assert_eq!(parse_balance("1000000000LAMPORTS").unwrap(), 1_000_000_000);
        assert_eq!(parse_balance("42lamports").unwrap(), 42);

        // No suffix → treat as SOL
        assert_eq!(parse_balance("1").unwrap(), 1_000_000_000);
        assert_eq!(parse_balance("0.1").unwrap(), 100_000_000);

        assert!(parse_balance("").is_err());
        assert!(parse_balance("abc").is_err());
        assert!(parse_balance("1.2.3SOL").is_err());
        assert!(parse_balance("SOL").is_err());

        // 0.000000001 SOL == 1 lamport
        assert_eq!(parse_balance("0.000000001SOL").unwrap(), 1);

        // Tiny fractions under one lamport get truncated down
        assert_eq!(parse_balance("0.0000000009SOL").unwrap(), 0);
    }
}
