//! Meta error which wraps all the submodule errors.
use {
    crate::{
        accounts_creator::AccountsCreatorError, accounts_file::StateLoaderError,
        blockhash_updater::BlockhashUpdaterError, csv_writer::CSVWriterError,
        leader_updater::Error as LeaderUpdaterError, yellowstone_subscriber::YellowstoneError,
    },
    solana_tpu_client_next::ConnectionWorkersSchedulerError,
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum RateLatencyToolError {
    #[error(transparent)]
    AccountsCreatorError(#[from] AccountsCreatorError),

    #[error(transparent)]
    ConnectionTasksSchedulerError(#[from] ConnectionWorkersSchedulerError),

    #[error(transparent)]
    StateLoaderError(#[from] StateLoaderError),

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
