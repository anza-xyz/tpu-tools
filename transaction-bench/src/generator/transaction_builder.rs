use {
    crate::cli::{InstructionPaddingConfig, TRANSFER_TRANSACTION_LOADED_ACCOUNTS_DATA_SIZE},
    solana_compute_budget_interface::ComputeBudgetInstruction,
    solana_hash::Hash,
    solana_instruction::Instruction,
    solana_keypair::Keypair,
    solana_message::{
        VersionedMessage,
        v1::{Message as V1Message, TransactionConfig},
    },
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    solana_system_interface::instruction as system_instruction,
    solana_transaction::{Transaction, versioned::VersionedTransaction},
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

    let tx: VersionedTransaction = Transaction::new_signed_with_payer(
        &instructions,
        Some(&payer.pubkey()),
        &signers,
        recent_blockhash,
    )
    .into();

    wincode::serialize(&tx).expect("serialize VersionedTransaction in send_batch")
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
    compute_unit_price: Option<u64>,
    use_txv1: bool,
) -> Vec<u8>
where
    S: Iterator<Item = &'a Keypair>,
    R: Iterator<Item = &'a Keypair>,
    L: Iterator<Item = &'a u64>,
{
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

    let tx = create_transfer_transaction(
        tx_payer,
        instructions,
        signers,
        recent_blockhash,
        transaction_cu_budget,
        instruction_padding_config,
        compute_unit_price,
        use_txv1,
    );

    wincode::serialize(&tx).expect("serialize VersionedTransaction in send_batch")
}

#[allow(clippy::too_many_arguments)]
fn create_transfer_transaction(
    tx_payer: &Pubkey,
    instructions: &mut Vec<Instruction>,
    signers: &[&Keypair],
    recent_blockhash: Hash,
    transaction_cu_budget: u32,
    instruction_padding_config: Option<&InstructionPaddingConfig>,
    compute_unit_price: Option<u64>,
    use_txv1: bool,
) -> VersionedTransaction {
    if use_txv1 {
        create_v1_transfer_transaction(
            tx_payer,
            instructions,
            signers,
            recent_blockhash,
            transaction_cu_budget,
            instruction_padding_config,
            compute_unit_price,
        )
    } else {
        create_legacy_transfer_transaction(
            tx_payer,
            instructions,
            signers,
            recent_blockhash,
            transaction_cu_budget,
            instruction_padding_config,
            compute_unit_price,
        )
    }
}

fn create_legacy_transfer_transaction(
    tx_payer: &Pubkey,
    instructions: &mut Vec<Instruction>,
    signers: &[&Keypair],
    recent_blockhash: Hash,
    transaction_cu_budget: u32,
    instruction_padding_config: Option<&InstructionPaddingConfig>,
    compute_unit_price: Option<u64>,
) -> VersionedTransaction {
    let mut compute_budget_instructions = vec![ComputeBudgetInstruction::set_compute_unit_limit(
        transaction_cu_budget,
    )];
    if let Some(compute_unit_price) = compute_unit_price {
        compute_budget_instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
            compute_unit_price,
        ));
    }
    if let Some(padding_config) = instruction_padding_config {
        compute_budget_instructions.push(
            ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(
                padding_config.loaded_accounts_data_size_limit,
            ),
        );
    }
    instructions.splice(0..0, compute_budget_instructions);

    Transaction::new_signed_with_payer(instructions, Some(tx_payer), signers, recent_blockhash)
        .into()
}

fn create_v1_transfer_transaction(
    tx_payer: &Pubkey,
    instructions: &[Instruction],
    signers: &[&Keypair],
    recent_blockhash: Hash,
    transaction_cu_budget: u32,
    instruction_padding_config: Option<&InstructionPaddingConfig>,
    compute_unit_price: Option<u64>,
) -> VersionedTransaction {
    let loaded_accounts_data_size_limit = instruction_padding_config
        .map(|padding_config| padding_config.loaded_accounts_data_size_limit)
        .unwrap_or(TRANSFER_TRANSACTION_LOADED_ACCOUNTS_DATA_SIZE);
    let mut config = TransactionConfig::empty()
        .with_compute_unit_limit(transaction_cu_budget)
        .with_loaded_accounts_data_size_limit(loaded_accounts_data_size_limit);
    if let Some(compute_unit_price) = compute_unit_price {
        config = config.with_priority_fee(compute_unit_price);
    }

    let message =
        V1Message::try_compile_with_config(tx_payer, instructions, recent_blockhash, config)
            .unwrap();
    VersionedTransaction::try_new(VersionedMessage::V1(message), signers).unwrap()
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
    use {super::*, solana_message::VersionedMessage, solana_pubkey::Pubkey};

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

    #[test]
    fn test_create_serialized_transfers_uses_legacy_by_default() {
        let tx = create_test_transfer(None, false);

        assert!(matches!(&tx.message, VersionedMessage::Legacy(_)));
        assert_eq!(tx.message.instructions().len(), 2);
    }

    #[test]
    fn test_create_serialized_transfers_adds_legacy_compute_unit_price() {
        let tx = create_test_transfer(Some(1), false);

        assert!(matches!(&tx.message, VersionedMessage::Legacy(_)));
        assert_eq!(tx.message.instructions().len(), 3);
    }

    #[test]
    fn test_create_serialized_transfers_uses_v1_when_enabled() {
        let tx = create_test_transfer(Some(1), true);
        let VersionedMessage::V1(message) = tx.message else {
            panic!("expected v1 message");
        };

        assert_eq!(message.config.compute_unit_limit, Some(600));
        assert_eq!(message.config.priority_fee, Some(1));
        assert_eq!(
            message.config.loaded_accounts_data_size_limit,
            Some(TRANSFER_TRANSACTION_LOADED_ACCOUNTS_DATA_SIZE)
        );
        assert_eq!(message.instructions.len(), 1);
    }

    fn create_test_transfer(
        compute_unit_price: Option<u64>,
        use_txv1: bool,
    ) -> VersionedTransaction {
        let sender = Keypair::new();
        let receiver = Keypair::new();
        let lamports = [1];
        let mut accounts_from = [&sender].into_iter();
        let mut accounts_to = [&receiver].into_iter();
        let mut lamports = lamports.iter();
        let mut instructions = Vec::new();
        let mut signers = Vec::new();

        let wire_transaction = create_serialized_transfers(
            &mut accounts_from,
            &mut accounts_to,
            &mut lamports,
            Hash::new_unique(),
            &mut instructions,
            &mut signers,
            1,
            600,
            None,
            compute_unit_price,
            use_txv1,
        );

        wincode::deserialize(&wire_transaction).unwrap()
    }
}
