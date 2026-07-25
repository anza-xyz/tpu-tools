use {
    log::info,
    solana_commitment_config::CommitmentConfig,
    solana_faucet::faucet::run_local_faucet_with_unique_port_for_tests,
    solana_fee_calculator::FeeRateGovernor,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_net_utils::SocketAddrSpace,
    solana_rate_latency_tool::{
        cli::{ExecutionParams, TxAnalysisParams},
        run_client::run_client,
    },
    solana_rent::Rent,
    solana_rpc::rpc::JsonRpcConfig,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_rpc_client_api::{
        config::RpcBlockConfig, response::transaction::versioned::VersionedTransaction,
    },
    solana_signer::Signer,
    solana_test_validator::TestValidatorGenesis,
    solana_tpu_client_next::SendTransactionStats,
    solana_tpu_tools_common::{
        accounts_file::create_ephemeral_accounts,
        cli::{AccountParams, LeaderTracker},
    },
    solana_transaction_status::{TransactionDetails, UiTransactionEncoding},
    spl_memo_interface::v3::id as spl_memo_id,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::runtime::Builder,
    tokio_util::sync::CancellationToken,
};

#[test]
fn test_transactions_sending() {
    agave_logger::setup_with("debug");

    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();

    let faucet_addr = run_local_faucet_with_unique_port_for_tests(mint_keypair);

    let test_validator = TestValidatorGenesis::default()
        .rpc_config(JsonRpcConfig {
            enable_rpc_transaction_history: true,
            enable_extended_tx_metadata_storage: true,
            ..JsonRpcConfig::default_for_test()
        })
        .fee_rate_governor(FeeRateGovernor::new(0, 0))
        .rent(Rent {
            lamports_per_byte_year: 1,
            exemption_threshold: 1.0,
            ..Rent::default()
        })
        .faucet_addr(Some(faucet_addr))
        .start_with_mint_address(mint_pubkey, SocketAddrSpace::Unspecified)
        .expect("validator start failed");

    let rpc_client = Arc::new(test_validator.get_async_rpc_client());
    let websocket_url = test_validator.rpc_pubsub_url();
    let tpu_addr = *(test_validator.tpu_quic());

    let rt = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");

    let cancel = CancellationToken::new();
    let stats = Arc::new(SendTransactionStats::default());
    let client_stats = stats.clone();
    let rpc_client_for_blocks = rpc_client.clone();
    let handle = rt.spawn(async move {
        let funding_key = Keypair::new();
        let funding_pubkey = funding_key.pubkey();
        // fund the payer account
        let latest_blockhash = get_latest_blockhash(rpc_client.as_ref()).await;
        let _ = rpc_client
            .request_airdrop_with_blockhash(&funding_pubkey, 100_000_000, &latest_blockhash)
            .await
            .expect("Airdrop request should not fail.");
        wait_for_balance(rpc_client.as_ref(), &funding_pubkey, 100_000_000).await;
        let account_params = AccountParams {
            num_payers: 16,
            payer_account_balance: 1000,
        };

        let accounts = create_ephemeral_accounts(
            rpc_client.clone(),
            funding_key,
            account_params.num_payers,
            account_params.payer_account_balance,
            true,
        )
        .await?;
        run_client(
            rpc_client,
            websocket_url,
            accounts,
            ExecutionParams {
                staked_identity_file: None,
                bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0),
                duration: Some(Duration::from_secs(2)),
                num_max_open_connections: 1,
                send_fanout: 1,
                send_interval: Duration::from_millis(50),
                compute_unit_price: Some(100),
                handshake_timeout: Duration::from_secs(2),
                leader_tracker: LeaderTracker::PinnedLeaderTracker { address: tpu_addr },
            },
            TxAnalysisParams {
                output_csv_file: None,
                connection_stats_csv_file: None,
                yellowstone_url: None,
                yellowstone_token: None,
                check_all_txs: false,
            },
            client_stats,
            cancel,
        )
        .await
    });

    rt.block_on(handle)
        .expect("Should not fail joining client task.")
        .expect("Should not fail running client.");

    let successfully_sent = stats.to_non_atomic().successfully_sent;
    assert!(
        successfully_sent > 0,
        "Expected client to successfully send at least one memo tx"
    );

    let num_memo_tx = rt.block_on(wait_for_confirmed_memo_txs(
        rpc_client_for_blocks.as_ref(),
        Duration::from_secs(30),
    ));

    assert!(
        num_memo_tx > 0,
        "Expected to receive at least one memo tx but got {num_memo_tx}"
    );
    drop(test_validator);
}

async fn get_latest_blockhash(client: &RpcClient) -> Hash {
    loop {
        match client.get_latest_blockhash().await {
            Ok(blockhash) => return blockhash,
            Err(err) => {
                info!("Couldn't get last blockhash: {err:?}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };
    }
}

async fn wait_for_balance(client: &RpcClient, pubkey: &solana_pubkey::Pubkey, target: u64) {
    for _ in 0..30 {
        if let Ok(balance) = client.get_balance(pubkey).await
            && balance >= target
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("Airdrop balance did not reach target {target} for {pubkey}");
}

async fn wait_for_confirmed_memo_txs(client: &RpcClient, timeout: Duration) -> u64 {
    let before = Instant::now();
    loop {
        let num_memo_tx = count_confirmed_memo_txs(client).await;
        if num_memo_tx > 0 || before.elapsed() >= timeout {
            return num_memo_tx;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn count_confirmed_memo_txs(client: &RpcClient) -> u64 {
    let Ok(blocks) = client
        .get_blocks_with_limit_and_commitment(0, 100, CommitmentConfig::confirmed())
        .await
    else {
        return 0;
    };

    let mut num_memo_tx = 0u64;
    for slot in blocks {
        let Ok(block) = client
            .get_block_with_config(
                slot,
                RpcBlockConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    transaction_details: Some(TransactionDetails::Full),
                    rewards: Some(false),
                    commitment: Some(CommitmentConfig::confirmed()),
                    max_supported_transaction_version: None,
                },
            )
            .await
        else {
            continue;
        };
        let Some(encoded_transactions) = block.transactions else {
            continue;
        };
        for encoded_tx in encoded_transactions {
            if let Some(tx) = encoded_tx.transaction.decode()
                && is_memo(tx)
            {
                num_memo_tx = num_memo_tx.saturating_add(1);
            }
        }
    }
    num_memo_tx
}
fn is_memo(tx: VersionedTransaction) -> bool {
    let message = &tx.message;
    let account_keys = message.static_account_keys();

    for instruction in message.instructions() {
        if instruction.program_id(account_keys) == &spl_memo_id() {
            if let Ok(s) = std::str::from_utf8(&instruction.data) {
                info!("Memo data: \"{s}\"");
            }
            return true;
        }
    }
    false
}
