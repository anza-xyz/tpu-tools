use {
    log::*,
    solana_hash::Hash,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    std::sync::Arc,
    thiserror::Error,
    tokio::{
        sync::watch,
        time::{self, Duration, Instant},
    },
};

/// Config was introduced for test purposes.
#[derive(Clone, Copy, Debug)]
struct BlockhashUpdaterConfig {
    /// How often request the new blockhash.
    update_interval: Duration,
    /// If we fail to update blockhash for this period of time, we give up and report error.
    stuck_interval: Duration,
    /// When start to report warnings about blockhash not updating.
    not_updating_interval: Duration,
    /// How often to report blockhash errors if any.
    error_report_interval: Duration,
}

impl Default for BlockhashUpdaterConfig {
    fn default() -> Self {
        Self {
            update_interval: Duration::from_secs(1),
            stuck_interval: Duration::from_secs(120),
            not_updating_interval: Duration::from_secs(30),
            error_report_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Error, Debug, PartialEq, Eq, Clone, Copy)]
pub enum BlockhashUpdaterError {
    #[error("Blockhash is stuck.")]
    BlockhashStuck,
}

pub struct BlockhashUpdater {
    rpc_client: Arc<RpcClient>,
    sender: watch::Sender<Hash>,
    config: BlockhashUpdaterConfig,
    last_blockhash: Hash,
}

impl BlockhashUpdater {
    pub fn new(rpc_client: Arc<RpcClient>, sender: watch::Sender<Hash>) -> Self {
        Self {
            rpc_client,
            sender,
            config: BlockhashUpdaterConfig::default(),
            last_blockhash: Hash::default(),
        }
    }

    #[cfg(test)]
    fn with_config(
        rpc_client: Arc<RpcClient>,
        sender: watch::Sender<Hash>,
        config: BlockhashUpdaterConfig,
    ) -> Self {
        Self {
            rpc_client,
            sender,
            config,
            last_blockhash: Hash::default(),
        }
    }

    pub async fn run(mut self) -> Result<(), BlockhashUpdaterError> {
        let mut blockhash_last_updated = Instant::now();
        let mut last_error_log = Instant::now();
        let mut interval = time::interval(self.config.update_interval);
        while !self.sender.is_closed() {
            interval.tick().await;

            if let Ok(new_blockhash) = self.rpc_client.get_latest_blockhash().await
                && new_blockhash != self.last_blockhash
            {
                self.last_blockhash = new_blockhash;
                if self.sender.send(new_blockhash).is_err() {
                    break;
                }
                blockhash_last_updated = Instant::now();
            }
            if blockhash_last_updated.elapsed() > self.config.stuck_interval {
                return Err(BlockhashUpdaterError::BlockhashStuck);
            } else if blockhash_last_updated.elapsed() > self.config.not_updating_interval
                && last_error_log.elapsed() >= self.config.error_report_interval
            {
                last_error_log = Instant::now();
                let last_updated_s = blockhash_last_updated.elapsed().as_secs();
                warn!("Blockhash is not updating for {last_updated_s} s.");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        serde_json::{self, json},
        solana_rpc_client::mock_sender::PUBKEY,
        solana_rpc_client_api::{
            request::RpcRequest,
            response::{Response, RpcBlockhash, RpcResponseContext},
        },
        solana_sha256_hasher::hash,
        std::collections::HashMap,
        tokio::{sync::watch, task, time::sleep},
    };

    // Lower than default values to avoid long running unit tests.
    fn test_config() -> BlockhashUpdaterConfig {
        BlockhashUpdaterConfig {
            update_interval: Duration::from_millis(200),
            stuck_interval: Duration::from_millis(600),
            not_updating_interval: Duration::from_millis(300),
            error_report_interval: Duration::from_millis(1000),
        }
    }

    #[tokio::test]
    async fn test_blockhash_updates_successfully() {
        let rpc_blockhash = hash(&[1u8]);
        let mut mocks = HashMap::new();
        mocks.insert(
            RpcRequest::GetLatestBlockhash,
            json!(Response {
                context: RpcResponseContext {
                    slot: 1,
                    api_version: None
                },
                value: json!(RpcBlockhash {
                    blockhash: rpc_blockhash.to_string(),
                    last_valid_block_height: 42,
                }),
            }),
        );
        let rpc_client = Arc::new(RpcClient::new_mock_with_mocks("".to_string(), mocks));
        let (sender, receiver) = watch::channel(Hash::default());
        let updater_config = test_config();
        let updater = BlockhashUpdater::with_config(rpc_client, sender, updater_config);
        let handle = task::spawn(async move { updater.run().await });
        // sleep to let updater task entering the update loop.
        sleep(updater_config.update_interval / 2).await;
        let blockhash = *receiver.borrow();
        assert_eq!(rpc_blockhash, blockhash);
        drop(receiver);
        let result = handle.await.expect("task should not panic.");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_blockhash_updates_stuck() {
        let rpc_client = Arc::new(RpcClient::new_mock("fails".to_string()));
        let (sender, receiver) = watch::channel(Hash::default());
        let updater = BlockhashUpdater::with_config(rpc_client, sender, test_config());
        let handle = task::spawn(async move { updater.run().await });
        assert_eq!(*receiver.borrow(), Hash::default());
        let err = handle.await.expect("task should not panic.");
        assert_eq!(err, Err(BlockhashUpdaterError::BlockhashStuck));
    }

    #[tokio::test]
    async fn test_blockhash_updates_stuck_recover() {
        // MockRpcClient will first return specified response and later will always send
        // the same PUBKEY hash.
        let expected_blockhash: Hash = PUBKEY.parse().unwrap();
        let mut mocks = HashMap::new();
        mocks.insert(
            RpcRequest::GetLatestBlockhash,
            json!(Response {
                context: RpcResponseContext {
                    slot: 1,
                    api_version: None
                },
                value: serde_json::value::Value::Null,
            }),
        );
        let rpc_client = Arc::new(RpcClient::new_mock_with_mocks("".to_string(), mocks));
        let (sender, receiver) = watch::channel(Hash::default());
        let updater_config = test_config();
        let updater = BlockhashUpdater::with_config(rpc_client, sender, updater_config);
        let handle = task::spawn(async move { updater.run().await });
        // sleep to let updater task entering the update loop.
        sleep(updater_config.update_interval / 2).await;
        let blockhash = *receiver.borrow();
        assert_eq!(
            Hash::default(),
            blockhash,
            "Cannot update blockhash because rpc_client returns Null."
        );

        sleep(updater_config.update_interval).await;
        let blockhash = *receiver.borrow();
        assert_eq!(expected_blockhash, blockhash);
        drop(receiver);
        let result = handle.await.expect("task should not panic.");
        assert!(result.is_ok());
    }
}
