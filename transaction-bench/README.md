## Overview

`solana-transaction-bench` is a load-generation tool for Solana validators and clusters. It
generates transfer transactions, and sends them over QUIC to current and upcoming TPU leaders.

For testnet, the recommended workflow is to create payer accounts once with `write-accounts`, then
reuse them with `read-accounts-run`. For private or local clusters, `run` can create ephemeral payer
accounts and start the benchmark in one command.

### Usage Examples

#### Local Validator

Use `run` on a private or local cluster when it is acceptable to create fresh payer accounts for a
single benchmark run. This example funds `256` payer accounts, validates them, and sends transfer
transactions for `30` seconds using a staked QUIC connection. It assumes the local validator config
lives at `../solana/config`, with the faucet payer at `faucet.json` and the validator identity at
`bootstrap-validator/identity.json`. `pinned-leader-tracker` sends all transactions to the chosen
node.

```shell
args=(
  -ul
  --authority "$validator_config_dir/faucet.json"
  --validate-accounts
  run
  --num-payers 256
  --payer-account-balance 1SOL
  --duration 30
  --staked-identity-file "$validator_config_dir/bootstrap-validator/identity.json"
  --transfer-tx-cu-budget 600
  pinned-leader-tracker "127.0.0.1:8002"
)
solana-transaction-bench "${args[@]}"
```

#### Cluster with saved accounts

On public clusters such as testnet, it is more convinient to create payer accounts once and reuse
them across benchmark runs. This avoids repeatedly funding accounts and makes runs easier to
restart. Use enough payer accounts for the load you want to generate: `1024` is a reasonable
starting point for high-throughput tests.

```shell
create_accounts_args=(
  -u "$URL"
  --authority "$FUNDER_KEYPAIR"
  --validate-accounts
  write-accounts
  --accounts-file accounts.json
  --num-payers 1024
  --payer-account-balance 1SOL
)
solana-transaction-bench "${create_accounts_args[@]}"
```

After `accounts.json` is created, use `read-accounts-run` to start sending transactions. A high
`--lamports-to-transfer` value gives the generator enough unique transfer amounts to avoid duplicate
transactions within generated batches. Here we use `ws-leader-tracker` to keep track of leaders
using websocket.

```shell
run_args=(
  -u "$URL"
  --authority "$FUNDER_KEYPAIR"
  read-accounts-run
  --accounts-file accounts.json
  --duration 30
  --staked-identity-file "$VALIDATOR_IDENTITY"
  --num-max-open-connections 16
  --workers-pull-size 8
  --send-fanout 1
  --transfer-tx-cu-budget 350
  --lamports-to-transfer 8096
  --num-send-instructions-per-tx 1
  ws-leader-tracker
)
solana-transaction-bench "${run_args[@]}"
```

#### Multiple Staked Identities

Pass `--staked-identity-file` more than once to spawn multiple
`tpu-client-next` instances. Repeat the same keypair to open multiple clients
under one identity, or pass different keypairs when you want to use distinct
stake allocations.

```shell
args=(
  -u "$URL"
  read-accounts-run
  --accounts-file accounts.json
  --duration 30
  --staked-identity-file ./identity-a.json
  --staked-identity-file ./identity-b.json
  --staked-identity-file ./identity-c.json
  --send-fanout 1
  ws-leader-tracker
)
solana-transaction-bench "${args[@]}"
```

#### Target TPS

Use `--target-tps` to pace transaction generation instead of sending as fast as
the generator and connection workers allow. When `--target-tps` is set and
`--tx-batch-size` is not set, the tool chooses a batch size for roughly 10
batches per second and adjusts the generator worker count for the requested
rate.

```shell
args=(
  -u "$URL"
  read-accounts-run
  --accounts-file accounts.json
  --duration 30
  --target-tps 5000
  --transfer-tx-cu-budget 600
  --send-fanout 2
  ws-leader-tracker
)
solana-transaction-bench "${args[@]}"
```

#### Large Transactions

Increase transaction size by adding more transfer instructions per transaction or by wrapping
transfers with the SPL instruction padding program. When instruction padding is enabled, the padding
program must exist on the target cluster; pass `--instruction-padding-program-id` if you are using a
custom deployment. This example derives the local program id from
`../solana/tmp/spl_instruction_padding.json`; set `INSTRUCTION_PADDING_PROGRAM_ID` or
`INSTRUCTION_PADDING_KEYPAIR` to use a different deployment. The tool checks that program account
before sending.

Note: multiple transfer instructions per transaction are supported without Transaction V1 by setting
`--num-send-instructions-per-tx`. The `--use-txv1` flag in this example only switches generated
transactions to Transaction V1 format. Transaction V1 is available only when the target Agave
cluster supports it and the `enable_tx_v1` feature is activated, so verify cluster support before
using `--use-txv1` on external clusters.

```shell
args=(
  -u "$URL"
  read-accounts-run
  --accounts-file accounts.json
  --duration 30
  --num-send-instructions-per-tx 4
  --transfer-tx-cu-budget 4000
  --instruction-padding-data-size 512
  --instruction-padding-program-id "$instruction_padding_program_id"
  --use-txv1
  --send-fanout 2
  ws-leader-tracker
)
solana-transaction-bench "${args[@]}"
```

<details>
<summary>How to install the padding program</summary>

```shell
git clone https://github.com/solana-program/instruction-padding.git
cd instruction-padding
cargo build-sbf
solana -u "$URL" program deploy spl_instruction_padding.so --program-id spl_instruction_padding.json -k config/faucet.json
```

</details>

#### Transaction Conflicts

Use `--tx-batch-size` with `--num-conflict-groups` to intentionally reuse destination accounts
within each generated batch. Lower conflict group counts create more account conflicts.
`--num-conflict-groups` must be no greater than `--num-send-instructions-per-tx * --tx-batch-size`.

```shell
args=(
  -u "$URL"
  read-accounts-run
  --accounts-file accounts.json
  --duration 30
  --tx-batch-size 64
  --num-conflict-groups 4
  --num-send-instructions-per-tx 1
  --transfer-tx-cu-budget 600
  --send-fanout 2
  ws-leader-tracker
)
solana-transaction-bench "${args[@]}"
```

#### Use yellowstone-grpc

Use `yellowstone-leader-tracker` when you want to receive slot updates from a Yellowstone gRPC
endpoint instead of the RPC websocket. The Yellowstone URL is a gRPC endpoint, and the token is
passed as the second positional argument.

```shell
args=(
  -u "$URL"
  --authority "$FUNDER_KEYPAIR"
  read-accounts-run
  --accounts-file accounts.json
  --duration 30
  --staked-identity-file "$VALIDATOR_IDENTITY"
  --send-fanout 1
  yellowstone-leader-tracker "$YELLOWSTONE_GRPC_URL" "$YELLOWSTONE_GRPC_TOKEN"
)
solana-transaction-bench "${args[@]}"
```
