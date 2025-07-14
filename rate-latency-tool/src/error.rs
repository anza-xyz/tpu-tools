//! Meta error which wraps all the submodule errors.
use {
    crate::{csv_writer::CSVWriterError, yellowstone_subscriber::YellowstoneError},
    solana_tpu_client_next::ConnectionWorkersSchedulerError,
    thiserror::Error,
    tools_common::{
        accounts_creator::Error as AccountsCreatorError, accounts_file::Error as AccountsFileError,
        blockhash_updater::BlockhashUpdaterError, leader_updater::Error as LeaderUpdaterError,
    },
};

#[derive(Debug, Error)]
pub enum RateLatencyToolError {
    #[error(transparent)]
    AccountsCreatorError(#[from] AccountsCreatorError),

    #[error(transparent)]
    ConnectionTasksSchedulerError(#[from] ConnectionWorkersSchedulerError),

    #[error(transparent)]
    StateLoaderError(#[from] AccountsFileError),

    #[error(transparent)]
    BlockhashUpdaterError(#[from] BlockhashUpdaterError),

    #[error(transparent)]
    CSVWriterError(#[from] CSVWriterError),

    #[error(transparent)]
    YellowstoneError(#[from] YellowstoneError),

    #[error("Failed to read keypair file")]
    KeypairReadFailure,

    #[error("Accounts validation failed")]
    AccountsValidationFailure,

    #[error("Could not find validator identity among staked nodes")]
    FindValidatorIdentityFailure,

    #[error("Tool finished unexpectedly")]
    UnexpectedError,

    #[error(transparent)]
    LeaderUpdaterError(#[from] LeaderUpdaterError),
}
