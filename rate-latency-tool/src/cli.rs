pub use tools_common::cli::LeaderTracker;
use {
    clap::{crate_description, crate_name, crate_version, value_parser, Args, Parser, Subcommand},
    solana_clap_v3_utils::input_parsers::parse_url,
    solana_commitment_config::CommitmentConfig,
    std::{net::SocketAddr, path::PathBuf},
    tokio::time::Duration,
    tools_common::cli::{
        parse_and_normalize_url, parse_duration_ms, parse_duration_sec, AccountParams,
        ReadAccounts, WriteAccounts,
    },
};

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
        value_parser = parse_and_normalize_url,
        help = "URL for Solana's JSON RPC or moniker (or their first letter):\n\
        [mainnet-beta, testnet, devnet, localhost]"
    )]
    pub json_rpc_url: String,

    #[clap(
        long,
        default_value = "confirmed",
        value_parser = value_parser!(CommitmentConfig),
        help = "Block commitment config for getting latest blockhash.\n"
    )]
    pub commitment_config: CommitmentConfig,

    // Cannot use value_parser to read keypair file because Keypair is not Clone.
    #[clap(
        long,
        help = "Keypair file of authority. If not provided, create a new one.\nIf authority has \
                insufficient funds, client will try airdrop."
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
        value_parser = parse_duration_sec,
        help = "If specified, limits the benchmark execution to the specified duration in seconds."
    )]
    pub duration: Option<Duration>,

    #[clap(
        long,
        value_parser = parse_duration_ms,
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
        help = "To how many future leaders the transactions should be sent. The connection fanout \
                is set send_fanout + 1."
    )]
    pub send_fanout: usize,

    #[clap(long, help = "Sets compute-unit-price for transactions.")]
    pub compute_unit_price: Option<u64>,

    #[clap(
        long,
        value_parser = parse_duration_sec,
        default_value = "2",
        help = "Handshake timeout."
    )]
    pub handshake_timeout: Duration,

    #[clap(subcommand)]
    pub leader_tracker: LeaderTracker,
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
        value_parser = parse_url,
        requires = "output-csv-file",
        help = "Yellowstone url."
    )]
    pub yellowstone_url: Option<String>,

    #[clap(long, requires = "yellowstone-url", help = "Yellowstone token.")]
    pub yellowstone_token: Option<String>,

    #[clap(
        long,
        requires = "yellowstone-url",
        help = "File to write mapping between slot and number of transactions in the \
                corresponding block."
    )]
    pub txs_per_block_file: Option<PathBuf>,
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
                payer_account_balance: LAMPORTS_PER_SOL,
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
                leader_tracker: LeaderTracker::WsLeaderTracker,
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
                yellowstone_token: None,
                txs_per_block_file: None,
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
        args.push("ws-leader-tracker");

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
        let (exec_args, mut execution_params) = get_common_execution_params(keypair_file_name);
        execution_params.leader_tracker = LeaderTracker::YellowstoneLeaderTracker {
            url: "http://localhost:1234".to_string(),
            token: Some("TOKEN".to_string()),
        };
        args.extend(exec_args.iter());
        let (analysis_args, analysis_params) = get_common_analysis_params();
        args.extend(analysis_args.iter());
        args.push("yellowstone-leader-tracker");
        args.push("http://localhost:1234");
        args.push("TOKEN");

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
