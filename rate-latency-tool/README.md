## Rate latency tool

Tool sends memo transactions avery `--send-interval` ms for the duration `--duration`.
Each memo has the following info: `transaction_id, generation_timestamp, target_slot`.
Optionally, tx fee might be specified.

What data we can collect from this

* `slot_latency = landed_slot - target_slot` where `landed_slot` we can get from network,
* Number of transactions being lost vs time
* `time_latency = timestamp - generation_timestamps` -- but not sure how. Maybe with some geyser magic.
* Measure txs reordering.

Some future features:
* add support of grpc subscription
* optionally use RPC instead of TPU to send txs

### Run without saving created payers

In this case tool creates accounts instantly and they are dropped after the run.
This is useful for tests on private cluster.

```rust
solana-rate-latency-tool -ul --authority config/faucet.json --validate-accounts run --duration 60 --num-payers 128 --payer-account-balance 10 --send-fanout 2 --send-interval 100 --staked-identity-file config/bootstrap-validator/identity.json
```

### Run with saving created payers

For mainnet and even testnet it is desirable to save created payer accounts.
This is achieved by the tool by first creating accounts and saving it to file `accounts.json`:

```rust
solana-rate-latency-tool -ul --authority config/faucet.json --validate-accounts write-accounts --accounts-file accounts.json --num-payers 256 --payer-account-balance 10
```

And later sending transactions using generated accounts:

```rust
solana-rate-latency-tool -ul --authority config/faucet.json --validate-accounts read-accounts-run --accounts-file accounts.json --send-interval 10 --duration 60
```
