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

    bincode::serialize(&tx).expect("serialize Transaction in send_batch")
}

pub(crate) fn create_serialized_transfers<'a, I, L>(
    payer: &mut I,
    lamports: &mut L,
    recent_blockhash: Hash,
    instructions: &mut Vec<Instruction>,
    signers: &mut Vec<&'a Keypair>,
    num_send_instructions_per_tx: usize,
    transaction_cu_budget: u32,
) -> Vec<u8>
where
    I: Iterator<Item = &'a Keypair>,
    L: Iterator<Item = &'a u64>,
{
    // set cu instruction must be the first instruction in the transaction.
    instructions.insert(
        0,
        ComputeBudgetInstruction::set_compute_unit_limit(transaction_cu_budget),
    );

    // The first keypair in payers batch is both the transaction payer and the
    // first transfer sender. This is desirable so the payers iterator management
    // can rely on batch size to deduce how much to forward the payer index across batches.
    let tx_payer_kp = payer.next().unwrap();
    let tx_payer = &tx_payer_kp.pubkey();
    signers.push(tx_payer_kp);

    // First transfer: sender is tx_payer_kp
    let receiver = payer.next().unwrap();
    instructions.push(system_instruction::transfer(
        tx_payer,
        &receiver.pubkey(),
        *lamports.next().unwrap(),
    ));

    for _ in 1..num_send_instructions_per_tx {
        let fee_payer: &Keypair = payer.next().unwrap();
        signers.push(fee_payer);

        let receiver = payer.next().unwrap();

        instructions.push(system_instruction::transfer(
            &fee_payer.pubkey(),
            &receiver.pubkey(),
            *lamports.next().unwrap(),
        ));
    }

    let tx =
        Transaction::new_signed_with_payer(instructions, Some(tx_payer), signers, recent_blockhash);

    bincode::serialize(&tx).expect("serialize Transaction in send_batch")
}
