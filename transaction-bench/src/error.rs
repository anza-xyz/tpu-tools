//! Meta error which wraps all the submodule errors.
use {
    crate::generator::transaction_generator::TransactionGeneratorError,
    solana_tpu_client_next::ConnectionWorkersSchedulerError,
    thiserror::Error,
    tools_common::{
        accounts_creator::Error as AccountsCreatorError, accounts_file::Error as AccountsFileError,
        blockhash_updater::BlockhashUpdaterError, leader_updater::Error as LeaderUpdaterError,
    },
};

#[derive(Debug, Error)]
pub enum BenchClientError {
    #[error(transparent)]
    AccountsCreatorError(#[from] AccountsCreatorError),

    #[error(transparent)]
    ConnectionTasksSchedulerError(#[from] ConnectionWorkersSchedulerError),

    #[error("Failed to read keypair file")]
    KeypairReadFailure,

    #[error("Accounts validation failed")]
    AccountsValidationFailure,

    #[error("Could not find validator identity among staked nodes")]
    FindValidatorIdentityFailure,

    #[error("Leader updater failed")]
    LeaderUpdaterError(#[from] LeaderUpdaterError),

    #[error(transparent)]
    AccountsFileError(#[from] AccountsFileError),

    #[error("Invalid CLI arguments: {0}")]
    InvalidCliArguments(String),

    #[error(transparent)]
    BlockhashUpdaterError(#[from] BlockhashUpdaterError),

    #[error(transparent)]
    TransactionGeneratorError(#[from] TransactionGeneratorError),

    #[error("Task {task_name} was cancelled or panicked: {reason}")]
    TaskJoinFailure { task_name: String, reason: String },
}
