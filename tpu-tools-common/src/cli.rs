use {
    clap::{Args, Subcommand},
    solana_clap_v3_utils::{
        input_parsers::parse_url_or_moniker, input_validators::normalize_to_url_if_moniker,
    },
    solana_native_token::LAMPORTS_PER_SOL,
    std::{net::SocketAddr, path::PathBuf},
    tokio::time::Duration,
};

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
#[clap(rename_all = "kebab-case")]
pub enum LeaderTracker {
    #[clap(
        about = "Use pinned address to send transactions to, which means we are not interested in \
                 leader slot updates."
    )]
    PinnedLeaderTracker { address: SocketAddr },

    #[clap(
        about = "Use old ws tracking code for slot updates. WS url is generated from the RPC url."
    )]
    LegacyLeaderTracker,

    #[clap(about = "Use ws for slot updates. WS url is generated from the RPC url.")]
    WsLeaderTracker,

    #[clap(about = "Use yellowstone grpc for slot updates instead of ws.")]
    YellowstoneLeaderTracker {
        /// gRPC endpoint URL (positional argument)
        url: String,
        /// gRPC token (optional)
        token: Option<String>,
    },

    #[clap(about = "Use custom slot updater geyser plugin which sends slot updates over UDP.")]
    CustomLeaderTracker { bind_address: SocketAddr },
}

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

#[derive(Args, Debug, PartialEq, Eq, Clone)]
#[clap(rename_all = "kebab-case")]
pub struct WriteAccounts {
    #[clap(long, help = "File to save the created accounts into.")]
    pub accounts_file: PathBuf,

    #[clap(flatten)]
    pub account_params: AccountParams,
}

#[derive(Args, Debug, PartialEq, Eq, Clone)]
#[clap(rename_all = "kebab-case")]
pub struct ReadAccounts {
    #[clap(long, help = "File to read the accounts from.")]
    pub accounts_file: PathBuf,
}

pub fn parse_and_normalize_url(addr: &str) -> Result<String, String> {
    match parse_url_or_moniker(addr) {
        Ok(parsed) => Ok(normalize_to_url_if_moniker(&parsed)),
        Err(e) => Err(format!("Invalid URL or moniker: {e}")),
    }
}

pub fn parse_duration_sec(s: &str) -> Result<Duration, &'static str> {
    s.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|_| "failed to parse duration in seconds")
}

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
