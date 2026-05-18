use {
    async_trait::async_trait,
    serde_json::{json, to_value},
    solana_commitment_config::CommitmentConfig,
    solana_rpc_client::{
        mock_sender::MockSender,
        nonblocking::rpc_client::RpcClient,
        rpc_client::RpcClientConfig,
        rpc_sender::{RpcSender, RpcTransportStats},
    },
    solana_rpc_client_api::{
        client_error::Result as ClientResult,
        request::RpcRequest,
        response::{Response, RpcBlockhash, RpcResponseContext},
    },
    solana_sha256_hasher::hash,
    solana_sdk_ids::system_program,
    std::sync::atomic::{AtomicU64, Ordering},
};

pub fn new_mock_rpc_client(commitment_config: CommitmentConfig) -> RpcClient {
    RpcClient::new_sender(
        TransactionBenchMockSender::default(),
        RpcClientConfig::with_commitment(commitment_config),
    )
}

struct TransactionBenchMockSender {
    inner: MockSender,
    blockhash_counter: AtomicU64,
}

impl Default for TransactionBenchMockSender {
    fn default() -> Self {
        Self {
            inner: MockSender::new("succeeds"),
            blockhash_counter: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl RpcSender for TransactionBenchMockSender {
    fn get_transport_stats(&self) -> RpcTransportStats {
        self.inner.get_transport_stats()
    }

    async fn send(
        &self,
        request: RpcRequest,
        params: serde_json::Value,
    ) -> ClientResult<serde_json::Value> {
        if request == RpcRequest::GetLatestBlockhash {
            let next = self.blockhash_counter.fetch_add(1, Ordering::Relaxed);
            let blockhash = hash(&next.to_le_bytes());
            return to_value(Response {
                context: RpcResponseContext {
                    slot: next,
                    api_version: None,
                },
                value: RpcBlockhash {
                    blockhash: blockhash.to_string(),
                    last_valid_block_height: next.saturating_add(150),
                },
            })
            .map_err(Into::into);
        }

        if request == RpcRequest::GetAccountInfo {
            return Ok(json!(Response {
                context: RpcResponseContext {
                    slot: 1,
                    api_version: None,
                },
                value: {
                    "data": ["", "base64"],
                    "executable": false,
                    "lamports": 1,
                    "owner": system_program::id().to_string(),
                    "rentEpoch": 0,
                    "space": 0,
                },
            }));
        }

        self.inner.send(request, params).await
    }

    fn url(&self) -> String {
        "transaction-bench-mock-rpc".to_string()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
    };

    #[tokio::test]
    async fn test_mock_rpc_client_returns_changing_blockhashes() {
        let rpc_client = new_mock_rpc_client(CommitmentConfig::confirmed());

        let first = rpc_client.get_latest_blockhash().await.unwrap();
        let second = rpc_client.get_latest_blockhash().await.unwrap();

        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn test_mock_rpc_client_delegates_other_requests() {
        let rpc_client = new_mock_rpc_client(CommitmentConfig::confirmed());

        let balance = rpc_client.get_balance(&solana_pubkey::Pubkey::new_unique()).await;

        assert!(balance.is_ok());
    }

    #[tokio::test]
    async fn test_mock_rpc_client_returns_account_info() {
        let rpc_client = new_mock_rpc_client(CommitmentConfig::confirmed());

        let account = rpc_client
            .get_account(&solana_pubkey::Pubkey::new_unique())
            .await
            .unwrap();

        assert_eq!(account.lamports, 1);
    }
}
