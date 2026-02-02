use {
    clap::{Args, Parser, Subcommand, crate_description, crate_name, crate_version, value_parser},
    solana_clap_v3_utils::{
        input_parsers::parse_url_or_moniker, input_validators::normalize_to_url_if_moniker,
    },
    solana_commitment_config::CommitmentConfig,
    std::{net::SocketAddr, path::PathBuf},
    tokio::time::Duration,
    tools_common::cli::{AccountParams, LeaderTracker, ReadAccounts, WriteAccounts},
};

fn parse_and_normalize_url(addr: &str) -> Result<String, String> {
    match parse_url_or_moniker(addr) {
        Ok(parsed) => Ok(normalize_to_url_if_moniker(&parsed)),
        Err(e) => Err(format!("Invalid URL or moniker: {e}")),
    }
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
        value_parser = parse_and_normalize_url,
        help = "URL for Solana's JSON RPC or moniker (or their first letter):\n\
        [mainnet-beta, testnet, devnet, localhost]"
    )]
    pub json_rpc_url: String,

    #[clap(
        long,
        default_value = "confirmed",
        value_parser = value_parser!(CommitmentConfig),
        help = "Block commitment config for getting latest blockhash.\n\
        [possible values: processed, confirmed, finalized]"
    )]
    pub commitment_config: CommitmentConfig,

    // Cannot use value_parser to read keypair file because Keypair is not Clone.
    #[clap(
        long,
        help = "Keypair file of authority. If not provided, create a new one.\nIf authority has \
                insufficient funds, client will try airdrop."
    )]
    pub authority: Option<PathBuf>,

    #[clap(
        long,
        help = "Validate the created accounts number, size, balance.\nMight be time consuming, so \
                recommended only for debugging purposes."
    )]
    pub validate_accounts: bool,

    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug, PartialEq, Eq)]
pub enum Command {
    #[clap(about = "Create accounts without saving them and run")]
    Run {
        #[clap(flatten)]
        account_params: AccountParams,

        #[clap(flatten)]
        execution_params: ExecutionParams,

        #[clap(flatten)]
        transaction_params: TransactionParams,
    },

    #[clap(about = "Read accounts from provided accounts file and run")]
    ReadAccountsRun {
        #[clap(flatten)]
        read_accounts: ReadAccounts,

        #[clap(flatten)]
        execution_params: ExecutionParams,

        #[clap(flatten)]
        transaction_params: TransactionParams,
    },

    #[clap(about = "Create accounts and save them to a file, skipping the execution")]
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
        value_parser = parse_duration,
        help = "If specified, limits the benchmark execution to the specified duration."
    )]
    pub duration: Option<Duration>,

    #[clap(
        long,
        default_value_t = 16,
        help = "Max number of connections to keep open."
    )]
    pub num_max_open_connections: usize,

    #[clap(
        long,
        default_value_t = 8,
        help = "Size of the workers pull, controls how many transactions batches are generated in \
                parallel."
    )]
    pub workers_pull_size: usize,

    #[clap(
        long,
        default_value_t = 1,
        help = "To how many future leaders the transactions should be sent. The connection fanout \
                is set send_fanout + 1."
    )]
    pub send_fanout: usize,

    #[clap(long, help = "Sets compute-unit-price for transactions.")]
    pub compute_unit_price: Option<u64>,

    #[clap(subcommand)]
    pub leader_tracker: LeaderTracker,
}

#[derive(Args, Clone, Debug, PartialEq, Eq)]
#[clap(rename_all = "kebab-case")]
pub struct TransactionParams {
    #[clap(flatten)]
    pub simple_transfer_tx_params: SimpleTransferTxParams,
    //TODO(klykov): memo
}

#[derive(Args, Clone, Debug, PartialEq, Eq)]
#[clap(rename_all = "kebab-case")]
pub struct SimpleTransferTxParams {
    #[clap(
        long,
        default_value = "513",
        value_parser = value_parser!(u64).range(513..),
        help = "Max lamports to transfer in a transfer transaction, we select a random value in the range [0, this value]\n\
                to provide more entropy for transactions.\n"
    )]
    pub lamports_to_transfer: u64,

    #[clap(long, default_value = "600", help = "Transfer transaction CU budget.")]
    pub transfer_tx_cu_budget: u32,

    #[clap(
        long,
        default_value = "1",
        help = "Number of send instructions per transaction."
    )]
    pub num_send_instructions_per_tx: usize,
}

fn parse_duration(s: &str) -> Result<Duration, &'static str> {
    s.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|_| "failed to parse duration")
}

pub fn build_cli_parameters() -> ClientCliParameters {
    ClientCliParameters::parse()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        clap::Parser,
        solana_native_token::LAMPORTS_PER_SOL,
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
                "--send-fanout",
                "2",
                "--compute-unit-price",
                "1000",
                "pinned-leader-tracker",
                "127.0.0.1:8009",
            ],
            ExecutionParams {
                staked_identity_file: Some(PathBuf::from(&keypair_file_name)),
                bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
                duration: Some(Duration::from_secs(120)),
                leader_tracker: LeaderTracker::PinnedLeaderTracker {
                    address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8009),
                },
                num_max_open_connections: 16,
                workers_pull_size: 8,
                send_fanout: 2,
                compute_unit_price: Some(1000),
            },
        )
    }

    #[test]
    fn test_run_command() {
        let keypair_file_name = "/home/testUser/masterKey.json";

        let mut args = vec![
            "test",
            "-ul",
            "--authority",
            keypair_file_name,
            "run",
            "--lamports-to-transfer",
            "1000",
            "--transfer-tx-cu-budget",
            "600",
        ];
        let (account_args, account_params) = get_common_account_params();
        args.extend(account_args.iter());
        let (exec_args, execution_params) = get_common_execution_params(keypair_file_name);
        args.extend(exec_args.iter());

        let expected_parameters = ClientCliParameters {
            json_rpc_url: "http://localhost:8899".to_string(),
            commitment_config: CommitmentConfig::confirmed(),
            command: Command::Run {
                transaction_params: TransactionParams {
                    simple_transfer_tx_params: SimpleTransferTxParams {
                        lamports_to_transfer: 1000,
                        transfer_tx_cu_budget: 600,
                        num_send_instructions_per_tx: 1,
                    },
                },
                account_params,
                execution_params,
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
            "--transfer-tx-cu-budget",
            "1000",
            "--num-send-instructions-per-tx",
            "2",
        ];
        let (exec_args, execution_params) = get_common_execution_params(keypair_file_name);
        args.extend(exec_args.iter());

        let expected_parameters = ClientCliParameters {
            json_rpc_url: "http://localhost:8899".to_string(),
            commitment_config: CommitmentConfig::confirmed(),
            command: Command::ReadAccountsRun {
                read_accounts: ReadAccounts {
                    accounts_file: accounts_file_name.into(),
                },

                transaction_params: TransactionParams {
                    simple_transfer_tx_params: SimpleTransferTxParams {
                        lamports_to_transfer: 513,
                        transfer_tx_cu_budget: 1000,
                        num_send_instructions_per_tx: 2,
                    },
                },
                execution_params,
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

    /// Check that cannot use `write` subcommand together with parameters from `TransactionParams`
    #[test]
    fn test_write_accounts_file_conflict() {
        let keypair_file_name = "/home/testUser/masterKey.json";
        let accounts_file_name = "/home/testUser/accountsFile.json";

        let mut args = vec![
            "test",
            "-ul",
            "--authority",
            keypair_file_name,
            "write-accounts",
            "--num-accounts-per-tx",
            "100",
            "--accounts-file",
            accounts_file_name,
        ];

        let (account_args, _account_params) = get_common_account_params();
        args.extend(account_args.iter());

        let cli = ClientCliParameters::try_parse_from(args);
        assert!(cli.is_err());
    }
}
