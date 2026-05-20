# solana-tpu-tools-common

Shared building blocks for Solana TPU tools.

This crate is used by `solana-transaction-bench` and `solana-rate-latency-tool` for common tasks
around payer account management, blockhash refresh, command-line parsing, and TPU leader tracking.

Most users should run the binaries directly. Use this crate when building a related TPU tool that
needs the same account setup or leader-tracking behavior.

## What This Crate Provides

- `accounts_file`: read and write payer account files, create ephemeral payer
  accounts, and create file-persisted payer accounts.
- `accounts_creator`: RPC-backed payer account creation with balance checks,
  airdrop support, batching, and partial-result persistence on failure.
- `cli`: shared Clap argument structs and parsers for account options, account
  files, URLs, durations, balances, and leader tracker selection.
- `blockhash_updater`: an async task that polls RPC for fresh blockhashes and
  publishes them through a `tokio::sync::watch` channel.
- `leader_updater`: a factory and traits for pinned, legacy websocket,
  websocket node-address-service, Yellowstone, and custom UDP/Geyser leader
  tracking.
- `yellowstone_leader_tracker`: a Yellowstone gRPC slot-event adapter for
  `solana-tpu-client-next` leader tracking.

## Example

```rust,no_run
use {
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_tpu_client_next::node_address_service::LeaderTpuCacheServiceConfig,
    solana_tpu_tools_common::{
        blockhash_updater::BlockhashUpdater,
        cli::LeaderTracker,
        leader_updater::create_leader_updater,
    },
    std::{net::SocketAddr, sync::Arc, time::Duration},
    tokio::sync::watch,
    tokio_util::sync::CancellationToken,
};

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let rpc_client = Arc::new(RpcClient::new("http://127.0.0.1:8899".to_string()));
    let cancel = CancellationToken::new();

    let initial_blockhash = rpc_client.get_latest_blockhash().await?;
    let (blockhash_sender, blockhash_receiver) = watch::channel(initial_blockhash);
    tokio::spawn(BlockhashUpdater::new(rpc_client.clone(), blockhash_sender).run());

    let leader_tracker = LeaderTracker::PinnedLeaderTracker {
        address: "127.0.0.1:8004".parse::<SocketAddr>()?,
    };
    let config = LeaderTpuCacheServiceConfig {
        lookahead_leaders: 4,
        refresh_nodes_info_every: Duration::from_secs(30),
        max_consecutive_failures: 5,
    };

    let mut leader_updater = create_leader_updater(
        rpc_client,
        leader_tracker,
        config,
        "ws://127.0.0.1:8900".to_string(),
        cancel,
    )
    .await?;

    let _current_blockhash = *blockhash_receiver.borrow();
    let _next_leaders = leader_updater.next_leaders(1);
    leader_updater.stop().await;
    Ok(())
}
```

## Stability

This crate follows the release cadence of the TPU tools workspace. APIs may change as
`solana-tpu-client-next`, Yellowstone gRPC, and Solana/Agave release candidate dependencies evolve.

## License

Apache-2.0
