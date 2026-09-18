//! TPU transaction submission helper shared by account-maintenance commands.

use {
    crate::{
        cli::LeaderTracker,
        leader_updater::{LeaderUpdaterFactory, create_leader_updater},
    },
    solana_keypair::Keypair,
    solana_net_utils::sockets::bind_to,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_signer::EncodableKey,
    solana_tpu_client_next::{
        Client, ClientBuilder, ClientError as TpuClientError, TransactionSender,
        client_builder::ClientBuilderError, node_address_service::LeaderTpuCacheServiceConfig,
    },
    std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, sync::Arc},
    thiserror::Error,
    tokio_util::sync::CancellationToken,
};

#[derive(Debug, Error)]
pub enum Error {
    /// TPU client could not be built.
    #[error(transparent)]
    TpuClientBuilderError(#[from] ClientBuilderError),

    /// TPU client request failed.
    #[error(transparent)]
    TpuClientError(#[from] TpuClientError),

    /// Leader updater creation failed.
    #[error(transparent)]
    LeaderUpdaterError(#[from] crate::leader_updater::Error),

    /// UDP bind failed.
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    /// A keypair file could not be read.
    #[error("Failed to read keypair file")]
    KeypairReadFailure,
}

/// Transport used to submit transactions.
#[derive(Clone)]
pub enum TransactionSubmitter {
    /// Submit and confirm transactions through RPC.
    Rpc,
    /// Submit transactions through TPU and confirm them through RPC.
    Tpu(TransactionSender),
}

/// Owns a TPU client and its leader-update service.
pub struct TpuTransactionClient {
    pub transaction_submitter: TransactionSubmitter,
    client: Client,
    leader_service: LeaderUpdaterFactory,
}

impl TpuTransactionClient {
    pub async fn shutdown(self) -> Result<(), Error> {
        let client_result = self.client.shutdown().await;
        let service_result = self.leader_service.shutdown().await;
        client_result?;
        service_result?;
        Ok(())
    }
}

pub async fn create_tpu_transaction_client(
    rpc_client: Arc<RpcClient>,
    leader_tracker: LeaderTracker,
    websocket_url: String,
    bind: SocketAddr,
    stake_identity_file: Option<PathBuf>,
    num_max_open_connections: NonZeroUsize,
    send_fanout: usize,
    cancel: CancellationToken,
) -> Result<TpuTransactionClient, Error> {
    let stake_identity = stake_identity_file
        .map(|path| Keypair::read_from_file(path).map_err(|_| Error::KeypairReadFailure))
        .transpose()?;
    let leader_service = create_leader_updater(
        rpc_client,
        leader_tracker,
        LeaderTpuCacheServiceConfig::default(),
        websocket_url,
        cancel.clone(),
    )
    .await?;
    let leader_updater = leader_service.create_updater().await?;
    let bind_socket = bind_to(bind.ip(), bind.port())?;
    let (transaction_sender, client) = ClientBuilder::new(leader_updater)
        .bind_socket(bind_socket)
        .identity(stake_identity.as_ref())
        .max_cache_size(num_max_open_connections)
        .leader_send_fanout(send_fanout)
        .cancel_token(cancel)
        .build()?;

    Ok(TpuTransactionClient {
        transaction_submitter: TransactionSubmitter::Tpu(transaction_sender),
        client,
        leader_service,
    })
}
