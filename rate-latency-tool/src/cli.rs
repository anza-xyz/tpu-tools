use {
    clap::{crate_description, crate_name, crate_version, Args, Parser, Subcommand},
    solana_clap_v3_utils::{
        input_parsers::{parse_url, parse_url_or_moniker},
        input_validators::normalize_to_url_if_moniker,
    },
    solana_commitment_config::CommitmentConfig,
    std::{net::SocketAddr, path::PathBuf},
    tokio::time::Duration,
};

fn normalize_to_url(addr: &str) -> Result<String, &'static str> {
    Ok(normalize_to_url_if_moniker(addr))
}

#[derive(Parser, Debug, PartialEq, Eq)]
#[clap(name = crate_name!(),
    version = crate_version!(),
    about = crate_description!(),
    rename_all = "kebab-case"
)]
pub struct ClientCliParameters {
    #[clap(
        long = "url",
        short = 'u',
        validator = parse_url_or_moniker,
        parse(try_from_str = normalize_to_url),
        help = "URL for Solana's JSON RPC or moniker (or their first letter):\n\
        [mainnet-beta, testnet, devnet, localhost]"
    )]
    pub json_rpc_url: String,

    #[clap(
        long,
        default_value = "confirmed",
        possible_values = &["processed", "confirmed", "finalized"],
        help = "Block commitment config for getting latest blockhash.\n"
    )]
    pub commitment_config: CommitmentConfig,

    // Cannot use value_parser to read keypair file because Keypair is not Clone.
    #[clap(
        long,
        help = "Keypair file of authority. If not provided, create a new one.\n\
                If authority has insufficient funds, client will try airdrop."
    )]
    pub authority: Option<PathBuf>,

    #[clap(long, help = "Validate the created accounts number and balance.")]
    pub validate_accounts: bool,

    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug, PartialEq, Eq)]
pub enum Command {
    #[clap(about = "Create accounts without saving them and run.")]
    Run {
        #[clap(flatten)]
        account_params: AccountParams,

        #[clap(flatten)]
        execution_params: ExecutionParams,

        #[clap(flatten)]
        analysis_params: TxAnalysisParams,
    },

    #[clap(about = "Read accounts from provided accounts file and run.")]
    ReadAccountsRun {
        #[clap(flatten)]
        read_accounts: ReadAccounts,

        #[clap(flatten)]
        execution_params: ExecutionParams,

        #[clap(flatten)]
        analysis_params: TxAnalysisParams,
    },

    #[clap(about = "Create accounts and save them to a file, skipping the execution.")]
    WriteAccounts(WriteAccounts),
}

#[derive(Args, Clone, Debug, PartialEq, Eq)]
#[clap(rename_all = "kebab-case")]
pub struct ExecutionParams {
    // Cannot use value_parser to read keypair file because Keypair is not Clone.
    #[clap(long, help = "validator identity for staked connection.")]
    pub staked_identity_file: Option<PathBuf>,

    /// Address to bind on, default will listen on all available interfaces, 0 that
    /// OS will choose the port.
    #[clap(long, help = "bind", default_value = "0.0.0.0:0")]
    pub bind: SocketAddr,

    #[clap(
        long,
        parse(try_from_str = parse_duration_sec),
        help = "If specified, limits the benchmark execution to the specified duration in seconds."
    )]
    pub duration: Option<Duration>,

    #[clap(
        long,
        parse(try_from_str = parse_duration_ms),
        help = "Interval between sent transactions in milliseconds."
    )]
    pub send_interval: Duration,

    #[clap(
        long,
        default_value_t = 16,
        help = "Max number of connections to keep open."
    )]
    pub num_max_open_connections: usize,

    #[clap(
        long,
        default_value_t = 1,
        help = "To how many future leaders the transactions should be sent. The connection fanout is set send_fanout + 1."
    )]
    pub send_fanout: usize,

    #[clap(long, help = "Sets compute-unit-price for transactions.")]
    pub compute_unit_price: Option<u64>,

    #[clap(
        long,
        parse(try_from_str = parse_duration_sec),
        default_value = "2",
        help = "Handshake timeout."
    )]
    pub handshake_timeout: Duration,

    #[clap(subcommand)]
    pub leader_tracker: LeaderTracker,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
#[clap(rename_all = "kebab-case")]
pub enum LeaderTracker {
    #[clap(
        help = "Use pinned address to send transactions to, which means we are not interested in leader slot updates."
    )]
    Pinned { address: SocketAddr },

    #[clap(
        help = "Use old ws tracking code for slot updates. WS url is generated from the RPC url."
    )]
    Legacy,

    #[clap(help = "Use ws for slot updates. WS url is generated from the RPC url.")]
    NodeAddressService,

    #[clap(help = "Use yellowstone grpc for slot updates instead of ws.")]
    Yellowstone { url: String },

    #[clap(help = "Use custom slot updater geyser plugin which sends slot updates over UDP.")]
    SlotUpdater { bind_address: SocketAddr },
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
        default_value = "1",
        help = "Payer account balance in SOL,\n\
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

#[derive(Args, Debug, PartialEq, Eq, Clone)]
#[clap(rename_all = "kebab-case")]
pub struct TxAnalysisParams {
    #[clap(
        long,
        requires = "yellowstone-url",
        help = "File to write received transaction data."
    )]
    pub output_csv_file: Option<PathBuf>,

    #[clap(
        long,
        validator = parse_url,
        requires = "output-csv-file",
        help = "Yellowstone url."
    )]
    pub yellowstone_url: Option<String>,
}

fn parse_duration_sec(s: &str) -> Result<Duration, &'static str> {
    s.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|_| "failed to parse duration in seconds")
}

fn parse_duration_ms(s: &str) -> Result<Duration, &'static str> {
    s.parse::<u64>()
        .map(Duration::from_millis)
        .map_err(|_| "failed to parse duration in milliseconds")
}

pub fn build_cli_parameters() -> ClientCliParameters {
    ClientCliParameters::parse()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        clap::Parser,
        std::net::{IpAddr, Ipv4Addr},
    };

    fn get_common_account_params() -> (Vec<&'static str>, AccountParams) {
        (
            vec!["--num-payers", "256", "--payer-account-balance", "1"],
            AccountParams {
                num_payers: 256,
                payer_account_balance: 1,
            },
        )
    }

    fn get_common_execution_params(keypair_file_name: &str) -> (Vec<&str>, ExecutionParams) {
        (
            vec![
                "--staked-identity-file",
                keypair_file_name,
                "--duration",
                "120",
                "--send-interval",
                "100",
                "--send-fanout",
                "3",
            ],
            ExecutionParams {
                staked_identity_file: Some(PathBuf::from(&keypair_file_name)),
                bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
                duration: Some(Duration::from_secs(120)),
                num_max_open_connections: 16,
                send_interval: Duration::from_millis(100),
                send_fanout: 3,
                compute_unit_price: None,
                handshake_timeout: Duration::from_secs(2),
                leader_tracker: LeaderTracker::NodeAddressService,
            },
        )
    }

    fn get_common_analysis_params() -> (Vec<&'static str>, TxAnalysisParams) {
        let csv_file = "/home/testUser/file.csv";
        let yellowstone_url = "http://127.0.0.1:10000";
        (
            vec![
                "--output-csv-file",
                &csv_file,
                "--yellowstone-url",
                &yellowstone_url,
            ],
            TxAnalysisParams {
                output_csv_file: Some(PathBuf::from(csv_file.to_string())),
                yellowstone_url: Some(yellowstone_url.to_string()),
            },
        )
    }

    #[test]
    fn test_run_command() {
        let keypair_file_name = "/home/testUser/masterKey.json";

        let mut args = vec!["test", "-ul", "--authority", keypair_file_name, "run"];
        let (exec_args, execution_params) = get_common_execution_params(keypair_file_name);
        args.extend(exec_args.iter());
        let (account_args, account_params) = get_common_account_params();
        args.extend(account_args.iter());
        let (analysis_args, analysis_params) = get_common_analysis_params();
        args.extend(analysis_args.iter());
        args.push("node-address-service");

        let expected_parameters = ClientCliParameters {
            json_rpc_url: "http://localhost:8899".to_string(),
            commitment_config: CommitmentConfig::confirmed(),
            command: Command::Run {
                account_params,
                execution_params,
                analysis_params,
            },
            authority: Some(PathBuf::from(&keypair_file_name)),
            validate_accounts: false,
        };
        let actual = ClientCliParameters::try_parse_from(args).unwrap();

        assert_eq!(actual, expected_parameters);
    }

    #[test]
    fn test_read_accounts_run_command() {
        let keypair_file_name = "/home/testUser/masterKey.json";
        let accounts_file_name = "/home/testUser/accountsFile.json";

        let mut args = vec![
            "test",
            "-ul",
            "--authority",
            keypair_file_name,
            "read-accounts-run",
            "--accounts-file",
            accounts_file_name,
        ];
        let (exec_args, execution_params) = get_common_execution_params(keypair_file_name);
        args.extend(exec_args.iter());
        let (analysis_args, analysis_params) = get_common_analysis_params();
        args.extend(analysis_args.iter());
        args.push("node-address-service");

        let expected_parameters = ClientCliParameters {
            json_rpc_url: "http://localhost:8899".to_string(),
            commitment_config: CommitmentConfig::confirmed(),
            command: Command::ReadAccountsRun {
                read_accounts: ReadAccounts {
                    accounts_file: accounts_file_name.into(),
                },
                execution_params,
                analysis_params,
            },
            authority: Some(PathBuf::from(&keypair_file_name)),
            validate_accounts: false,
        };
        let cli = ClientCliParameters::try_parse_from(args);
        assert!(cli.is_ok(), "Unexpected error {:?}", cli.err());
        let actual = cli.unwrap();

        assert_eq!(actual, expected_parameters);
    }

    #[test]
    fn test_write_accounts_command() {
        let keypair_file_name = "/home/testUser/masterKey.json";
        let accounts_file_name = "/home/testUser/accountsFile.json";

        let mut args = vec![
            "test",
            "-ul",
            "--authority",
            keypair_file_name,
            "write-accounts",
            "--accounts-file",
            accounts_file_name,
        ];

        let (account_args, account_params) = get_common_account_params();
        args.extend(account_args.iter());

        let expected_parameters = ClientCliParameters {
            json_rpc_url: "http://localhost:8899".to_string(),
            commitment_config: CommitmentConfig::confirmed(),
            command: Command::WriteAccounts(WriteAccounts {
                accounts_file: accounts_file_name.into(),
                account_params,
            }),
            authority: Some(PathBuf::from(&keypair_file_name)),
            validate_accounts: false,
        };
        let cli = ClientCliParameters::try_parse_from(args);
        assert!(cli.is_ok(), "Unexpected error {:?}", cli.err());
        let actual = cli.unwrap();

        assert_eq!(actual, expected_parameters);
    }
}
