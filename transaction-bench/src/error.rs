//! Meta error which wraps all the submodule errors.
use {
    solana_tpu_client_next::ConnectionWorkersSchedulerError,
    thiserror::Error,
    tools_common::{
        accounts_creator::Error as AccountsCreatorError, accounts_file::Error as AccountsFileError,
        leader_updater::Error as LeaderUpdaterError,
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
}
