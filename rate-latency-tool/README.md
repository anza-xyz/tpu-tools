## Rate latency tool

Tool sends memo transactions avery `--send-interval` ms for the duration `--duration`.
Each memo has the following info: `transaction_id, generation_timestamp, target_slot`.
Optionally, tx fee might be specified.

What data we can collect from this

* `slot_latency = landed_slot - target_slot` where `landed_slot` we can get from network,
* Number of transactions being lost vs time
* `time_latency = received_timestamp - sent_timestamp` which, offcourse, includes many latencies.
* Measure txs reordering.

### Run without saving created payers (only on private cluster)

In this case tool creates accounts instantly and they are dropped after the run.
This is useful for tests on private cluster.

```rust
solana-rate-latency-tool -ul --authority config/faucet.json --validate-accounts run --duration 60 --num-payers 128 --payer-account-balance 10 --send-fanout 2 --send-interval 100 --staked-identity-file config/bootstrap-validator/identity.json
```

### Run with saving created payers (on testnet, mainnet)

For mainnet and even testnet it is desirable to save created payer accounts.
This is achieved by the tool by first creating accounts and saving it to file `accounts.json`:

```rust
solana-rate-latency-tool -ut --authority "funder.json" --validate-accounts write-accounts --accounts-file accounts.json --num-payers 8 --payer-account-balance 1
```

And later sending transactions using generated accounts:

```rust
TOOL="/solana-rate-latency-tool"
FUNDER="funder.json"
IDENTITY="validator_identity.json" # for staked connection
DURATION=600

$TOOL -ut --authority $FUNDER --validate-accounts read-accounts-run  --accounts-file accounts.json --staked-identity-file $IDENTITY --send-interval 50 --duration $DURATION --compute-unit-price 10 --yellowstone-url "http://api.testnet.solana.com" --send-fanout $FANOUT  --output-csv-file out.csv yellowstone-leader-tracker "http://api.testnet.solana.com"  2> err.txt
```

### How to analyze the results

The result of the execution is file specified by `--output-csv-file`. This file has csv format and might be analyzed elsewhere. As an example, we provide a jupyter notebook which can be found in the scripts folder.

### How to setup local validator with geyser plugin (yellostone)

For geyser plugin to work with validator it is important that versions of rust match.
To build yellostone with agave from master, I use this branch https://github.com/KirillLykov/yellowstone-grpc/tree/klykov/update-to-agave-master.

When running validator, you need to add one additional cla: ` --geyser-plugin-config yellowstone-grpc/yellowstone-grpc-geyser/config.json`.
The config file contains path to the `libyellowstone_grpc_geyser.so` file, check that it is correct.
Beside of that, change the option `"replay_stored_slots"` because by default it is 0.


