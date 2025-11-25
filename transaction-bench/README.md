## Client to stress-test validator

Client application that sends transactions using QUIC protocol to the TPU port. Firstly, current
tool creates accounts necessary for the transaction generation. Secondly, it generates transactions
sends them to the upcoming leaders.

### To run with solana-test-validator

 Run the tool:

```shell
args=(
  -u "$ENDPOINT"
  --authority "$HOME/$ID_FILE"
  --staked-identity-file "$HOME/$CLIENT_NODE_ID_FILE"
  --duration "$DURATION"
  --num-payers 256
  --payer-account-balance 1
  --transaction-cu-budget 600
  #--validate-accounts -- if you want to check that the program has been deployed and accounts have been successfully created.
)
solana-transaction-bench "${args[@]}" 2> err.txt
```

### transaction-bench for stress testing

To create a high load on the your cluster of choice, you should create enough payers, lets say 1024,
and use high value for `--lamports-to-transfer` to avoid dedup:

```shell
./solana-transaction-bench-vanila -u 'http://<clusternmae>.rpcpool.com/<token>' --authority funder.json --validate-accounts write-accounts --accounts-file accounts-vanila.json --num-payers 1024 --payer-account-balance 1

./solana-transaction-bench-vanila -u 'http://<clusternmae>.rpcpool.com/<token>' --authority funder.json read-accounts-run --duration 300 --transfer-tx-cu-budget 350 --accounts-file ./accounts-vanila.json --staked-identity-file ./validator-identity.json --num-max-open-connections 256 --workers-pull-size 8 --lamports-to-transfer 8096 --num-send-instructions-per-tx 1 --send-fanout 2 ws-leader-tracker
```

This will make a similar effect to running bench-tps but with higher TPS.
