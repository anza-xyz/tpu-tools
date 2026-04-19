use {
    clap::{Args, Parser, Subcommand, crate_description, crate_name, crate_version, value_parser},
    solana_clap_v3_utils::{
        input_parsers::parse_url_or_moniker, input_validators::normalize_to_url_if_moniker,
    },
    solana_commitment_config::CommitmentConfig,
    solana_pubkey::Pubkey,
    std::{
        net::SocketAddr,
        num::{NonZeroU64, NonZeroUsize},
        path::PathBuf,
    },
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
        value_parser = value_parser!(NonZeroU64),
        help = "Optional global target send rate in transactions per second. When set, \
                transaction-bench switches to paced sending."
    )]
    pub target_tps: Option<NonZeroU64>,

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

    #[clap(flatten)]
    pub padding_params: InstructionPaddingParams,
    //TODO(klykov): memo
}

#[derive(Args, Clone, Debug, PartialEq, Eq)]
#[clap(rename_all = "kebab-case")]
pub struct InstructionPaddingParams {
    #[clap(
        long,
        help = "If set, wraps all transfer instructions in the instruction padding program, with \
                the given amount of padding bytes in instruction data."
    )]
    pub instruction_padding_data_size: Option<u32>,

    #[clap(
        long,
        requires = "instruction_padding_data_size",
        help = "Optionally specify the instruction padding program id. Defaults to the SPL \
                instruction padding program."
    )]
    pub instruction_padding_program_id: Option<Pubkey>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstructionPaddingConfig {
    pub program_id: Pubkey,
    pub data_size: u32,
    pub loaded_accounts_data_size_limit: u32,
}

// In case of plain transfer transaction, set loaded account data size to 30 KiB.
// It is large enough yet smaller than 32 KiB page size, so it would cost 0 extra CU.
const TRANSFER_TRANSACTION_LOADED_ACCOUNTS_DATA_SIZE: u32 = 30 * 1024;
// In case of padding program usage, we need to take into account program size too.
const PADDING_PROGRAM_ACCOUNT_DATA_SIZE: u32 = 28 * 1024;

fn get_padded_transaction_loaded_accounts_data_size() -> u32 {
    TRANSFER_TRANSACTION_LOADED_ACCOUNTS_DATA_SIZE + PADDING_PROGRAM_ACCOUNT_DATA_SIZE
}

impl TransactionParams {
    pub fn instruction_padding_config(&self) -> Option<InstructionPaddingConfig> {
        self.padding_params
            .instruction_padding_data_size
            .map(|data_size| InstructionPaddingConfig {
                program_id: self
                    .padding_params
                    .instruction_padding_program_id
                    .unwrap_or(spl_instruction_padding_interface::ID),
                data_size,
                loaded_accounts_data_size_limit: get_padded_transaction_loaded_accounts_data_size(),
            })
    }
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

    #[clap(
        long,
        value_parser = value_parser!(NonZeroUsize),
        help = "Number of transactions per batch. Required when using --num-conflict-groups."
    )]
    pub tx_batch_size: Option<NonZeroUsize>,

    #[clap(
        long,
        requires = "tx_batch_size",
        value_parser = value_parser!(NonZeroUsize),
        help = "Number of unique destination accounts per batch.\n\
                When set, destinations repeat to create account conflicts.\n\
                Lower value = more conflicts. Default: all destinations unique."
    )]
    pub num_conflict_groups: Option<NonZeroUsize>,
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
                target_tps: None,
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
                        tx_batch_size: None,
                        num_conflict_groups: None,
                    },
                    padding_params: InstructionPaddingParams {
                        instruction_padding_data_size: None,
                        instruction_padding_program_id: None,
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
                        tx_batch_size: None,
                        num_conflict_groups: None,
                    },
                    padding_params: InstructionPaddingParams {
                        instruction_padding_data_size: None,
                        instruction_padding_program_id: None,
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
    fn test_instruction_padding_config_defaults_program_id() {
        let params = TransactionParams {
            simple_transfer_tx_params: SimpleTransferTxParams {
                lamports_to_transfer: 513,
                transfer_tx_cu_budget: 600,
                num_send_instructions_per_tx: 1,
                tx_batch_size: None,
                num_conflict_groups: None,
            },
            padding_params: InstructionPaddingParams {
                instruction_padding_data_size: Some(128),
                instruction_padding_program_id: None,
            },
        };

        let padding_config = params.instruction_padding_config().unwrap();

        assert_eq!(
            padding_config.program_id,
            spl_instruction_padding_interface::ID
        );
        assert_eq!(padding_config.data_size, 128);
        assert_eq!(padding_config.loaded_accounts_data_size_limit, 58 * 1024);
    }

    #[test]
    fn test_target_tps_execution_param() {
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
            "--target-tps",
            "100",
        ];
        let (account_args, account_params) = get_common_account_params();
        args.extend(account_args.iter());
        let (exec_args, mut execution_params) = get_common_execution_params(keypair_file_name);
        args.extend(exec_args.iter());
        execution_params.target_tps = Some(NonZeroU64::new(100).unwrap());

        let actual = ClientCliParameters::try_parse_from(args).unwrap();
        let Command::Run {
            account_params: actual_account_params,
            execution_params: actual_execution_params,
            ..
        } = actual.command
        else {
            panic!("expected run command");
        };

        assert_eq!(actual_account_params, account_params);
        assert_eq!(actual_execution_params, execution_params);
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

    #[test]
    fn test_conflict_groups_requires_tx_batch_size() {
        let keypair_file_name = "/home/testUser/masterKey.json";
        let (account_args, _account_params) = get_common_account_params();
        let (exec_args, _execution_params) = get_common_execution_params(keypair_file_name);

        let mut base_args = vec!["test", "-ul", "--authority", keypair_file_name, "run"];
        base_args.extend(account_args.iter());

        // ok: both flags present
        let mut args = base_args.clone();
        args.extend(["--tx-batch-size", "64", "--num-conflict-groups", "4"]);
        args.extend(exec_args.iter());
        assert!(ClientCliParameters::try_parse_from(args).is_ok());

        // err: num-conflict-groups without tx-batch-size
        let mut args = base_args.clone();
        args.extend(["--num-conflict-groups", "4"]);
        args.extend(exec_args.iter());
        assert!(ClientCliParameters::try_parse_from(args).is_err());

        // err: num-conflict-groups = 0
        let mut args = base_args.clone();
        args.extend(["--tx-batch-size", "64", "--num-conflict-groups", "0"]);
        args.extend(exec_args.iter());
        assert!(ClientCliParameters::try_parse_from(args).is_err());
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
