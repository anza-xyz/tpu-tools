use {
    log::info,
    solana_commitment_config::CommitmentConfig,
    solana_faucet::faucet::run_local_faucet_with_unique_port_for_tests,
    solana_fee_calculator::FeeRateGovernor,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_net_utils::SocketAddrSpace,
    solana_pubsub_client::pubsub_client::PubsubClient,
    solana_rent::Rent,
    solana_rpc::{rpc::JsonRpcConfig, rpc_pubsub_service::PubSubConfig},
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_rpc_client_api::{
        config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter},
        response::RpcBlockUpdateError,
    },
    solana_sdk_ids::system_program,
    solana_signer::Signer,
    solana_test_validator::TestValidatorGenesis,
    solana_transaction_3::versioned::VersionedTransaction,
    solana_transaction_bench::{
        cli::{
            ExecutionParams, InstructionPaddingParams, SimpleTransferTxParams, TransactionParams,
        },
        run_client::run_client,
    },
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::Duration,
    },
    tokio::runtime::Builder,
    tools_common::{
        accounts_file::create_ephemeral_accounts,
        cli::{AccountParams, LeaderTracker},
    },
};

#[test]
fn test_transactions_sending() {
    agave_logger::setup_with("debug");

    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();

    let faucet_addr = run_local_faucet_with_unique_port_for_tests(mint_keypair);

    let test_validator = TestValidatorGenesis::default()
        .pubsub_config(PubSubConfig {
            enable_block_subscription: true,
            ..PubSubConfig::default()
        })
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

    let (mut block_subscribe_client, receiver) = PubsubClient::block_subscribe(
        test_validator.rpc_pubsub_url(),
        RpcBlockSubscribeFilter::All,
        Some(RpcBlockSubscribeConfig {
            commitment: Some(CommitmentConfig::confirmed()),
            // Keep default light settings to reduce chances of unavailable full block payloads.
            encoding: None,
            transaction_details: None,
            show_rewards: None,
            max_supported_transaction_version: None,
        }),
    )
    .unwrap();

    let handle = rt.spawn(async move {
        let funding_key = Keypair::new();
        let funding_pubkey = funding_key.pubkey();
        // fund the payer account
        let latest_blockhash = get_latest_blockhash(rpc_client.as_ref()).await;
        rpc_client
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
            TransactionParams {
                simple_transfer_tx_params: SimpleTransferTxParams {
                    lamports_to_transfer: 513,
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
            ExecutionParams {
                staked_identity_file: None,
                bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0),
                duration: Some(Duration::from_secs(5)),
                target_tps: None,
                num_max_open_connections: 1,
                workers_pull_size: 1,
                send_fanout: 1,
                compute_unit_price: Some(100),
                num_tpu_clients: 1,
                leader_tracker: LeaderTracker::PinnedLeaderTracker { address: tpu_addr },
            },
        )
        .await
    });

    let mut num_txs = 0;
    for _ in 0..10 {
        receiver.try_iter().for_each(|response| {
            if let Some(err) = response.value.err {
                // sometimes block is not ready, see
                // https://github.com/solana-labs/solana/issues/33462
                assert_eq!(err, RpcBlockUpdateError::BlockStoreError);
            }
            if let Some(block) = response.value.block
                && let Some(encoded_transactions) = block.transactions
            {
                for encoded_tx in encoded_transactions {
                    let tx = encoded_tx.transaction.decode();
                    if let Some(tx) = tx
                        && is_transfer(&tx)
                        && tx.message.instructions().len() == 2
                    {
                        num_txs += 1;
                    }
                }
            }
        });
        std::thread::sleep(Duration::from_secs(1));
    }

    rt.block_on(handle)
        .expect("Should not fail joining client task.")
        .expect("Should not fail running client.");

    assert!(
        num_txs > 0,
        "Expected to receive >0 transfer txs but got {num_txs}"
    );

    // If we don't drop the test_validator, the blocking web socket service
    // won't return, and the `block_subscribe_client` won't shut down
    drop(test_validator);
    block_subscribe_client.shutdown().unwrap();
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

fn is_transfer(tx: &VersionedTransaction) -> bool {
    let message = &tx.message;
    let account_keys = message.static_account_keys();

    message
        .instructions()
        .iter()
        .any(|instruction| instruction.program_id(account_keys) == &system_program::id())
}
