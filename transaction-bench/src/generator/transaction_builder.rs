use {
    solana_compute_budget_interface::ComputeBudgetInstruction, solana_hash::Hash,
    solana_instruction::Instruction, solana_keypair::Keypair, solana_signer::Signer,
    solana_system_interface::instruction as system_instruction, solana_transaction::Transaction,
};

#[allow(dead_code)]
pub(crate) fn create_serialized_signed_transaction(
    payer: &Keypair,
    recent_blockhash: Hash,
    mut instructions: Vec<Instruction>,
    additional_signers: Vec<&Keypair>,
    transaction_cu_budget: u32,
) -> Vec<u8> {
    let set_cu_instruction =
        ComputeBudgetInstruction::set_compute_unit_limit(transaction_cu_budget);

    // set cu instruction must be the first instruction in the transaction.
    instructions.insert(0, set_cu_instruction);

    let mut signers = vec![payer];
    signers.extend(additional_signers);

    let tx = Transaction::new_signed_with_payer(
        &instructions,
        Some(&payer.pubkey()),
        &signers,
        recent_blockhash,
    );

    wincode::serialize(&tx).expect("serialize Transaction in send_batch")
}

pub(crate) fn create_serialized_transfers<'a, S, R, L>(
    accounts_from: &mut S,
    accounts_to: &mut R,
    lamports: &mut L,
    recent_blockhash: Hash,
    instructions: &mut Vec<Instruction>,
    signers: &mut Vec<&'a Keypair>,
    num_send_instructions_per_tx: usize,
    transaction_cu_budget: u32,
) -> Vec<u8>
where
    S: Iterator<Item = &'a Keypair>,
    R: Iterator<Item = &'a Keypair>,
    L: Iterator<Item = &'a u64>,
{
    instructions.insert(
        0,
        ComputeBudgetInstruction::set_compute_unit_limit(transaction_cu_budget),
    );

    // First account-from is also the transaction fee payer
    let tx_payer_kp = accounts_from.next().unwrap();
    let tx_payer = &tx_payer_kp.pubkey();
    signers.push(tx_payer_kp);

    // First transfer: account-from = tx_payer_kp, account-to from iterator
    let receiver = accounts_to.next().unwrap();
    instructions.push(system_instruction::transfer(
        tx_payer,
        &receiver.pubkey(),
        *lamports.next().unwrap(),
    ));

    // Additional transfers for multi-instruction transactions
    for ((sender, receiver), amount) in accounts_from
        .by_ref()
        .zip(accounts_to.by_ref())
        .zip(lamports.by_ref())
        .take(num_send_instructions_per_tx.saturating_sub(1))
    {
        signers.push(sender);
        instructions.push(system_instruction::transfer(
            &sender.pubkey(),
            &receiver.pubkey(),
            *amount,
        ));
    }

    let tx =
        Transaction::new_signed_with_payer(instructions, Some(tx_payer), signers, recent_blockhash);

    wincode::serialize(&tx).expect("serialize Transaction in send_batch")
}
