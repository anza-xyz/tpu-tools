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
