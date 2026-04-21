use {
    crate::cli::InstructionPaddingConfig,
    rand::{Rng, thread_rng},
    solana_compute_budget_interface::ComputeBudgetInstruction,
    solana_hash::Hash,
    solana_instruction::Instruction,
    solana_keypair::Keypair,
    solana_signer::Signer,
    solana_system_interface::instruction as system_instruction,
    solana_transaction::Transaction,
    spl_instruction_padding_interface::instruction::wrap_instruction,
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_serialized_transfers<'a, S, R, L>(
    accounts_from: &mut S,
    accounts_to: &mut R,
    lamports: &mut L,
    recent_blockhash: Hash,
    instructions: &mut Vec<Instruction>,
    signers: &mut Vec<&'a Keypair>,
    num_send_instructions_per_tx: usize,
    transaction_cu_budget: u32,
    instruction_padding_config: Option<&InstructionPaddingConfig>,
    compute_unit_price: u64,
    random_compute_unit_price_max: u64,
) -> Vec<u8>
where
    S: Iterator<Item = &'a Keypair>,
    R: Iterator<Item = &'a Keypair>,
    L: Iterator<Item = &'a u64>,
{
    if let Some(padding_config) = instruction_padding_config {
        instructions.insert(
            0,
            ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(
                padding_config.loaded_accounts_data_size_limit,
            ),
        );
    }

    let base = compute_unit_price.max(1);
    let random_component = if random_compute_unit_price_max == 0 {
        0
    } else {
        thread_rng().gen_range(0..=random_compute_unit_price_max)
    };
    let price = base.saturating_add(random_component);

    instructions.insert(
        0,
        ComputeBudgetInstruction::set_compute_unit_limit(transaction_cu_budget),
    );
    instructions.insert(1, ComputeBudgetInstruction::set_compute_unit_price(price));

    // First account-from is also the transaction fee payer
    let tx_payer_kp = accounts_from.next().unwrap();
    let tx_payer = &tx_payer_kp.pubkey();
    signers.push(tx_payer_kp);

    // First transfer: account-from = tx_payer_kp, account-to from iterator
    let receiver = accounts_to.next().unwrap();
    let transfer_instruction =
        system_instruction::transfer(tx_payer, &receiver.pubkey(), *lamports.next().unwrap());
    instructions.push(maybe_wrap_instruction(
        transfer_instruction,
        instruction_padding_config,
    ));

    // Additional transfers for multi-instruction transactions
    for ((sender, receiver), amount) in accounts_from
        .by_ref()
        .zip(accounts_to.by_ref())
        .zip(lamports.by_ref())
        .take(num_send_instructions_per_tx.saturating_sub(1))
    {
        signers.push(sender);
        let transfer_instruction =
            system_instruction::transfer(&sender.pubkey(), &receiver.pubkey(), *amount);
        instructions.push(maybe_wrap_instruction(
            transfer_instruction,
            instruction_padding_config,
        ));
    }

    let tx =
        Transaction::new_signed_with_payer(instructions, Some(tx_payer), signers, recent_blockhash);

    wincode::serialize(&tx).expect("serialize Transaction in send_batch")
}

fn maybe_wrap_instruction(
    instruction: Instruction,
    instruction_padding_config: Option<&InstructionPaddingConfig>,
) -> Instruction {
    match instruction_padding_config {
        Some(padding_config) => wrap_instruction(
            padding_config.program_id,
            instruction,
            vec![],
            padding_config.data_size,
        )
        .expect("Could not create padded instruction"),
        None => instruction,
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_pubkey::Pubkey};

    #[test]
    fn test_maybe_wrap_instruction_keeps_transfer_when_disabled() {
        let from = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        let transfer_instruction = system_instruction::transfer(&from, &to, 1);

        let instruction = maybe_wrap_instruction(transfer_instruction.clone(), None);

        assert_eq!(instruction, transfer_instruction);
    }

    #[test]
    fn test_maybe_wrap_instruction_uses_padding_program_when_enabled() {
        let from = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        let transfer_instruction = system_instruction::transfer(&from, &to, 1);
        let padding_program_id = Pubkey::new_unique();
        let padding_config = InstructionPaddingConfig {
            program_id: padding_program_id,
            data_size: 64,
            loaded_accounts_data_size_limit: 58 * 1024,
        };

        let instruction = maybe_wrap_instruction(transfer_instruction, Some(&padding_config));

        assert_eq!(instruction.program_id, padding_program_id);
        assert!(!instruction.data.is_empty());
    }
}
