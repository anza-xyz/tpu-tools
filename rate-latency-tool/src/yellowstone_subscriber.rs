use {
    crate::csv_writer::CSVRecord,
    futures::stream::StreamExt,
    log::{debug, error, info, trace, warn},
    solana_pubkey::Pubkey,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_signature::Signature,
    solana_time_utils::timestamp,
    std::{
        collections::{HashMap, HashSet},
        fmt::{self, Display, Formatter},
        io,
        path::PathBuf,
        str::FromStr,
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
        vec,
    },
    thiserror::Error,
    tokio::{fs, sync::mpsc::Sender},
    tokio_util::sync::CancellationToken,
    yellowstone_grpc_client::{
        ClientTlsConfig, GeyserGrpcBuilderError, GeyserGrpcClient, GeyserGrpcClientError,
        Interceptor,
    },
    yellowstone_grpc_proto::{
        geyser::{
            CommitmentLevel, SlotStatus, SubscribeRequest, SubscribeRequestFilterBlocksMeta,
            SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions, SubscribeUpdateTransaction,
            subscribe_update::UpdateOneof,
        },
        tonic::transport::Certificate,
    },
};

#[derive(Debug, Error)]
pub enum YellowstoneError {
    #[error(transparent)]
    Builder(#[from] GeyserGrpcBuilderError),

    #[error(transparent)]
    GeyserGrpcClientError(#[from] GeyserGrpcClientError),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("Transaction update doesn't have transaction data.")]
    EmptyTransactionUpdate,

    #[error("Unexpected error.")]
    UnexpectedError,
}

pub(crate) struct ClientConfig {
    ca_certificate: Option<PathBuf>,
    yellowstone_url: String,
    x_token: Option<String>,
    max_decoding_message_size: usize,
    timeout: Duration,
}

pub(crate) async fn create_geyser_client(
    ClientConfig {
        ca_certificate,
        yellowstone_url,
        x_token,
        max_decoding_message_size,
        timeout,
    }: ClientConfig,
) -> Result<GeyserGrpcClient<impl Interceptor>, YellowstoneError> {
    let mut tls_config = ClientTlsConfig::new().with_native_roots();
    if let Some(path) = ca_certificate {
        let bytes = fs::read(path).await?;
        tls_config = tls_config.ca_certificate(Certificate::from_pem(bytes));
    }

    // TODO(klykov): not sure if I need to set up all these or the defaults
    // are sane enough.
    let builder = GeyserGrpcClient::build_from_shared(yellowstone_url)?
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .x_token(x_token)?
        .tls_config(tls_config)?
        .max_decoding_message_size(max_decoding_message_size)
        .connect_timeout(timeout)
        .keep_alive_timeout(timeout)
        .http2_adaptive_window(true)
        .initial_stream_window_size(9_000_000)
        .initial_connection_window_size(100_000_000)
        .tcp_nodelay(true)
        .timeout(timeout);
    let client = builder.connect().await?;
    Ok(client)
}

pub(crate) fn create_client_config(
    yellowstone_url: &str,
    yellowstone_token: Option<&str>,
) -> ClientConfig {
    ClientConfig {
        ca_certificate: None, // TODO: set this to a default CA certificate path
        yellowstone_url: yellowstone_url.to_string(),
        x_token: yellowstone_token.map(|token| token.to_string()),
        max_decoding_message_size: 1024 * 1024 * 1024,
        timeout: Duration::from_secs(30), // 30 seconds
    }
}

fn build_request(payers_pubkeys: &[Pubkey], check_all_transactions: bool) -> SubscribeRequest {
    let client_name = "rate-latency-tool".to_string();
    let mut transactions = HashMap::<String, SubscribeRequestFilterTransactions>::new();
    let mut slots = HashMap::new();
    let mut entry = HashMap::new();

    if check_all_transactions {
        info!("Subscribing to all transactions.");
        transactions.insert(
            client_name.clone(),
            SubscribeRequestFilterTransactions::default(),
        );
    } else {
        transactions.insert(
            client_name.clone(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: payers_pubkeys.iter().map(|pk| pk.to_string()).collect(),
                account_exclude: vec![],
                account_required: vec![],
            },
        );
    }
    slots.insert(
        client_name.clone(),
        SubscribeRequestFilterSlots {
            interslot_updates: Some(true),
            ..Default::default()
        },
    );
    entry.insert(client_name.clone(), SubscribeRequestFilterEntry::default());

    let mut blocks_meta: HashMap<String, SubscribeRequestFilterBlocksMeta> = HashMap::new();
    blocks_meta.insert(client_name.clone(), SubscribeRequestFilterBlocksMeta {});

    SubscribeRequest {
        accounts: HashMap::new(),
        slots,
        transactions,
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta,
        entry,
        commitment: Some(CommitmentLevel::Confirmed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    }
}

pub async fn run_yellowstone_subscriber(
    yellowstone_url: &str,
    yellowstone_token: Option<&str>,
    account_pubkeys: &[Pubkey],
    block_tx_sender: Sender<CSVRecord>,
    rpc_client: Arc<RpcClient>,
    write_all_txs: bool,
    cancel: CancellationToken,
) -> Result<(), YellowstoneError> {
    info!("run_yellowstone_subscriber_all_txs");
    let target_accounts: Arc<HashSet<Pubkey>> = Arc::new(account_pubkeys.iter().cloned().collect());
    let jito_tip_accounts = Arc::new(get_jito_tip_accounts(rpc_client).await);
    let amms_programs = Arc::new(prop_amms_programs());

    let client_config = create_client_config(yellowstone_url, yellowstone_token);
    let mut client = create_geyser_client(client_config).await?;

    let request = build_request(account_pubkeys, write_all_txs);

    let mut stream = client.subscribe_once(request).await?;

    let main_loop = async move {
        let mut recording_map = HashMap::<u64, SlotRecording>::default();
        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => {
                    let received_subscr_timestamp: Option<u64> = msg
                        .created_at
                        .and_then(|ts| ts.try_into().ok())
                        .and_then(|sys_time: SystemTime| sys_time.duration_since(UNIX_EPOCH).ok())
                        .map(|duration| duration.as_millis() as u64);
                    let received_timestamp = Some(timestamp());

                    let Some(msg) = msg.update_oneof else {
                        warn!("Update not found in the message.");
                        continue;
                    };
                    match msg {
                        UpdateOneof::Transaction(tx) => {
                            trace!("Received transaction info: {tx:?}");
                            if let Some(recording) = recording_map.get_mut(&tx.slot) {
                                debug!("Adding tx to recording for slot {}", tx.slot);
                                recording.add_transaction(
                                    tx,
                                    received_timestamp,
                                    received_subscr_timestamp,
                                );
                            } else {
                                trace!(
                                    "skipping txs for slot {} because we start tool between slot \
                                     updates.",
                                    tx.slot
                                );
                            }
                        }
                        UpdateOneof::Entry(entry_update) => {
                            trace!("Received entry info: {entry_update:?}");
                            if let Some(recording) = recording_map.get_mut(&entry_update.slot) {
                                recording.add_entry_update(EntryRecord {
                                    index: entry_update.index,
                                    executed_transaction_count: entry_update
                                        .executed_transaction_count,
                                });
                            } else {
                                // this happens because we start tool between slot updates.
                                trace!("skipping entries for slot {}", entry_update.slot);
                            }
                        }
                        UpdateOneof::Slot(grpc_slot) => {
                            trace!("Received slot info: {grpc_slot:?}");
                            // Process slot status
                            let slot_status = grpc_slot.status();
                            match slot_status {
                                SlotStatus::SlotConfirmed | SlotStatus::SlotFinalized => {
                                    debug!(
                                        "Received slot status {:?} for slot {}",
                                        slot_status, grpc_slot.slot
                                    );
                                    // End of slot is handled in BlockMeta
                                }
                                SlotStatus::SlotFirstShredReceived
                                | SlotStatus::SlotCreatedBank => {
                                    trace!("Create recording for slot {}", grpc_slot.slot);
                                    recording_map.entry(grpc_slot.slot).or_insert_with(|| {
                                        SlotRecording {
                                            slot: grpc_slot.slot,
                                            txs: Vec::new(),
                                            entries: Vec::new(),
                                        }
                                    });
                                }
                                SlotStatus::SlotDead => {
                                    trace!("Removing recording for dead slot {}", grpc_slot.slot);
                                    let _ = recording_map.remove(&grpc_slot.slot);
                                }
                                _ => {}
                            }
                        }
                        UpdateOneof::BlockMeta(block_meta) => {
                            let executed_transaction_count = block_meta.executed_transaction_count;
                            let entry_count = block_meta.entries_count;
                            let slot = block_meta.slot;

                            trace!(
                                "Received block meta for slot {slot}, \
                                 {executed_transaction_count}, {entry_count}"
                            );
                            if let Some(recording) = recording_map.get_mut(&slot) {
                                trace!(
                                    "Received txs: {}, expected: {}",
                                    recording.txs.len(),
                                    executed_transaction_count
                                );
                                let mut recording = recording_map.remove(&slot).unwrap();
                                tokio::spawn({
                                    let target_accounts = target_accounts.clone();
                                    let jito_tip_accounts = jito_tip_accounts.clone();
                                    let amms_programs = amms_programs.clone();
                                    let csv_sender = block_tx_sender.clone();
                                    async move {
                                        let ticks = recording.collect_ticks();
                                        recording.sort_txs();
                                        for tx in recording.txs {
                                            let tx = try_build_csv_record_with_classification(
                                                tx,
                                                &target_accounts,
                                                &jito_tip_accounts,
                                                &amms_programs,
                                            );
                                            if let Ok(mut tx) = tx {
                                                let Some(tx_index_in_block) = tx.index_in_block
                                                else {
                                                    error!("Transaction index in block is missing");
                                                    continue;
                                                };
                                                tx.tick = Some(ticks[tx_index_in_block as usize]);
                                                tx.num_transaction_in_block =
                                                    Some(executed_transaction_count as usize);

                                                if let Err(e) = csv_sender.send(tx).await {
                                                    error!("Failed to send transactions: {e}");
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                        }
                        _ => {
                            debug!("Received an unsupported update type, ignore it.");
                        }
                    }
                }
                Err(error) => {
                    error!("Unexpected yellowstone error: {error:?}");
                    return Err(YellowstoneError::UnexpectedError);
                }
            }
        }
        Ok(())
    };
    if let Some(res) = cancel.run_until_cancelled(main_loop).await {
        return res;
    }

    Ok(())
}

/// Extracts log line which starts with `Program log: Memo` from given log
/// lines.
fn extract_memo_line<T: AsRef<str>>(logs: &[T]) -> Option<&str> {
    logs.iter()
        .map(AsRef::as_ref)
        .find(|line| line.starts_with("Program log: Memo"))
}

/// Parse log message string having format `"Program log: Memo (len <LEN>):
/// \"<TX_ID>,<SLOT>,<TIMESTAMP_MS>\"",`
fn parse_memo_log(log: &str) -> Option<(usize, u64, u64)> {
    // Find the part inside quotes
    let quoted = log.split('"').nth(1)?;
    let mut parts = quoted.split(',');

    let transaction_id = parts.next()?.parse::<usize>().ok()?;
    let slot = parts.next()?.parse::<u64>().ok()?;
    let timestamp_ms = parts.next()?.parse::<u64>().ok()?;

    Some((transaction_id, slot, timestamp_ms))
}

struct TxWithTimestamps {
    tx: SubscribeUpdateTransaction,
    received_timestamp: Option<u64>,
    received_subscr_timestamp: Option<u64>,
}

struct SlotRecording {
    slot: u64,
    txs: Vec<TxWithTimestamps>,
    entries: Vec<EntryRecord>,
}

#[derive(Clone, Debug, Default)]
struct EntryRecord {
    index: u64,
    executed_transaction_count: u64,
}

impl SlotRecording {
    fn add_transaction(
        &mut self,
        tx: SubscribeUpdateTransaction,
        received_timestamp: Option<u64>,
        received_subscr_timestamp: Option<u64>,
    ) {
        self.txs.push(TxWithTimestamps {
            tx,
            received_timestamp,
            received_subscr_timestamp,
        });
    }

    fn add_entry_update(&mut self, entry: EntryRecord) {
        self.entries.push(entry);
    }

    fn collect_ticks(&mut self) -> Vec<u64> {
        debug!(
            "Collecting attributed transactions for slot {}, num entries {}",
            self.slot,
            self.entries.len()
        );

        self.entries.sort_by_key(|e| e.index);
        let ticks = find_tx_ticks(self.entries.as_slice());
        ticks
    }

    fn sort_txs(&mut self) {
        self.txs
            .sort_by_key(|tx| tx.tx.transaction.as_ref().map(|t| t.index).unwrap_or(0));
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TxType {
    Vote,
    // too hard to detect
    // need first to download data from Jito's API
    // JitoBundle,
    TargetTpu,
    JitoTip,
    PropAMM(PropAMM),
    UnclassifiedNonVote,
    Unknown,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PropAMM {
    HumidifFi,
    SolFi,
    Tessera,
    GoonFi,
    ZeroFi,
}

impl Display for TxType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let s = match self {
            TxType::Vote => "Vote",
            TxType::TargetTpu => "TargetTpu",
            TxType::JitoTip => "JitoTip",
            TxType::PropAMM(amm) => match amm {
                PropAMM::HumidifFi => "HumidiFi",
                PropAMM::SolFi => "SolFi",
                PropAMM::Tessera => "Tessera",
                PropAMM::GoonFi => "GoonFi",
                PropAMM::ZeroFi => "ZeroFi",
            },
            TxType::UnclassifiedNonVote => "UnclassifiedNonVote",
            TxType::Unknown => "Unknown",
        };
        write!(f, "{s}")
    }
}

fn try_build_csv_record_with_classification(
    TxWithTimestamps {
        tx,
        received_timestamp,
        received_subscr_timestamp,
    }: TxWithTimestamps,
    target_accounts: &Arc<HashSet<Pubkey>>,
    jito_tip_accounts: &Arc<HashSet<Pubkey>>,
    amms_programs: &Arc<HashMap<Pubkey, PropAMM>>,
) -> Result<CSVRecord, YellowstoneError> {
    let received_slot = tx.slot;
    let Some(tx) = tx.transaction else {
        return Err(YellowstoneError::EmptyTransactionUpdate);
    };
    let Some(meta) = tx.meta else {
        return Err(YellowstoneError::EmptyTransactionUpdate);
    };
    let signature = Signature::try_from(tx.signature.as_slice())
        .unwrap()
        .to_string();
    let index_in_block = Some(tx.index);

    let tx_type = {
        let mut tx_type = if tx.is_vote {
            TxType::Vote
        } else {
            TxType::UnclassifiedNonVote
        };

        if let Some(tx) = tx.transaction {
            if let Some(message) = tx.message {
                let accounts = message
                    .account_keys
                    .iter()
                    .map(|key_bytes| Pubkey::try_from(key_bytes.as_slice()).unwrap());
                for account in accounts {
                    if let Some(amm) = amms_programs.get(&account) {
                        tx_type = TxType::PropAMM(amm.clone());
                        break;
                    }
                    if target_accounts.contains(&account) {
                        tx_type = TxType::TargetTpu;
                        break;
                    }
                    if jito_tip_accounts.contains(&account) {
                        tx_type = TxType::JitoTip;
                        break;
                    }
                }
            }
        }
        tx_type
    };

    let memo_log = extract_memo_line(&meta.log_messages);
    if let Some(memo_log) = memo_log {
        if let Some((transaction_id, sent_slot, sent_timestamp)) = parse_memo_log(memo_log) {
            return Ok(CSVRecord {
                signature,
                transaction_id: Some(transaction_id),
                sent_slot: Some(sent_slot),
                received_slot: Some(received_slot),
                sent_timestamp: Some(sent_timestamp),
                received_timestamp,
                received_subscr_timestamp,
                index_in_block,
                tx_status: vec![],
                tick: None,
                tx_type: Some(tx_type.to_string()),
                num_transaction_in_block: None,
            });
        }
    }
    // not your tx
    Ok(CSVRecord {
        signature,
        transaction_id: None,
        sent_slot: None,
        received_slot: Some(received_slot),
        sent_timestamp: None,
        received_timestamp,
        received_subscr_timestamp,
        index_in_block,
        tx_status: vec![],
        tick: None,
        tx_type: Some(tx_type.to_string()),
        num_transaction_in_block: None,
    })
}

async fn get_jito_tip_accounts(rpc_client: Arc<RpcClient>) -> HashSet<Pubkey> {
    let jito_tip_program_id: Pubkey =
        Pubkey::from_str("T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt")
            .expect("Valid jit program pubkey");

    let response = rpc_client.get_program_accounts(&jito_tip_program_id).await;
    if let Err(e) = &response {
        error!("Failed to get Jito tip accounts: {e}");
        return HashSet::new();
    }
    response
        .unwrap()
        .into_iter()
        .map(|(pubkey, _account)| pubkey)
        .collect()
}

fn prop_amms_programs() -> HashMap<Pubkey, PropAMM> {
    let mut map = HashMap::new();
    map.insert(
        Pubkey::from_str("9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp").unwrap(),
        PropAMM::HumidifFi,
    );
    map.insert(
        Pubkey::from_str("SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe").unwrap(),
        PropAMM::SolFi,
    );
    map.insert(
        Pubkey::from_str("TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH").unwrap(),
        PropAMM::Tessera,
    );
    map.insert(
        Pubkey::from_str("goonERTdGsjnkZqWuVjs73BZ3Pb9qoCUdBUL17BnS5j").unwrap(),
        PropAMM::GoonFi,
    );
    map.insert(
        Pubkey::from_str("ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY").unwrap(),
        PropAMM::ZeroFi,
    );

    map
}

//  `entries` must be sorted by ids retrurns vector of ticks per transaction index
#[allow(clippy::arithmetic_side_effects)]
fn find_tx_ticks(entries: &[EntryRecord]) -> Vec<u64> {
    let mut num_txs_per_tick = Vec::with_capacity(64);
    let mut cur_num_txs = 0u64;
    let mut total_num_txs: usize = 0;
    for entry in entries.iter() {
        if entry.executed_transaction_count == 0 {
            num_txs_per_tick.push(cur_num_txs);
            cur_num_txs = 0;
        } else {
            cur_num_txs += entry.executed_transaction_count;
            total_num_txs += entry.executed_transaction_count as usize;
        }
    }
    if total_num_txs == 0 {
        return vec![];
    }
    if cur_num_txs > 0 {
        // if the last entry is not zero for whatever reason.
        num_txs_per_tick.push(cur_num_txs);
    }

    let mut ticks = vec![64; total_num_txs];
    let mut last_tx_index_in_tick = num_txs_per_tick[0];
    let mut tick = 0;
    for (tx_index, tx_tick) in ticks.iter_mut().enumerate() {
        while tx_index >= last_tx_index_in_tick as usize {
            tick += 1;
            last_tx_index_in_tick += num_txs_per_tick[tick];
        }
        *tx_tick = tick as u64;
    }
    ticks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(index: u64, executed_transaction_count: u64) -> EntryRecord {
        EntryRecord {
            index,
            executed_transaction_count,
        }
    }

    #[test]
    fn assigns_ticks_across_multiple_non_uniform_ticks() {
        let entries = vec![
            make_entry(0, 1),
            make_entry(1, 2),
            make_entry(2, 0),
            make_entry(3, 1),
            make_entry(4, 0),
            make_entry(5, 2),
            make_entry(6, 0),
        ];
        assert_eq!(find_tx_ticks(&entries), vec![0, 0, 0, 1, 2, 2]);
    }

    #[test]
    fn tick_boundary_uses_next_tick_size_not_previous_one() {
        let entries = vec![
            make_entry(0, 1),
            make_entry(1, 0),
            make_entry(2, 2),
            make_entry(3, 0),
            make_entry(4, 2),
            make_entry(5, 0),
        ];
        assert_eq!(find_tx_ticks(&entries), vec![0, 1, 1, 2, 2]);
    }

    #[test]
    fn handles_sparse_transaction_indices_without_panic() {
        let entries = vec![
            make_entry(0, 1),
            make_entry(1, 0),
            make_entry(2, 1),
            make_entry(3, 0),
        ];
        assert_eq!(find_tx_ticks(&entries), vec![0, 1]);
    }

    #[test]
    fn handles_multiple_consecutive_zero_entries() {
        // Two consecutive zero-entry ticks before any transactions; the first actual tx should be in tick 2.
        let entries = vec![
            make_entry(0, 0),
            make_entry(1, 0),
            make_entry(2, 3),
            make_entry(3, 0),
            make_entry(4, 0),
            make_entry(5, 1),
        ];
        let ticks = find_tx_ticks(&entries);
        assert_eq!(ticks, vec![2, 2, 2, 4]);
    }

    #[test]
    fn returns_empty_for_no_entries() {
        let entries: Vec<EntryRecord> = vec![];
        assert_eq!(find_tx_ticks(&entries), Vec::<u64>::new());
    }

    #[test]
    fn single_tick_without_zero_markers() {
        // No zero-count entries; everything should be tick 0.
        let entries = vec![make_entry(0, 3)];
        assert_eq!(find_tx_ticks(&entries), vec![0, 0, 0]);
    }

    #[test]
    fn trailing_non_zero_is_counted() {
        // No zero after the last batch; last tick still captured.
        let entries = vec![
            make_entry(0, 2),
            make_entry(1, 0), // tick 0
            make_entry(2, 3), // tick 1 (no trailing zero marker)
        ];
        assert_eq!(find_tx_ticks(&entries), vec![0, 0, 1, 1, 1]);
    }

    #[test]
    fn many_consecutive_empty_ticks_mid_slot() {
        // Multiple empty ticks in the middle should advance tick id even without txs.
        let entries = vec![
            make_entry(0, 2), // tick 0
            make_entry(1, 0),
            make_entry(2, 0), // empty tick 1
            make_entry(3, 0), // empty tick 2
            make_entry(4, 1), // tick 3 starts here
            make_entry(5, 0),
        ];
        assert_eq!(find_tx_ticks(&entries), vec![0, 0, 3]);
    }

    #[test]
    fn long_first_tick_then_boundary() {
        // Check boundary condition when tx_index == boundary (should move to next tick).
        let entries = vec![
            make_entry(0, 4),
            make_entry(1, 0), // tick 0 ends at index 3
            make_entry(2, 1),
            make_entry(3, 0),
        ];
        assert_eq!(find_tx_ticks(&entries), vec![0, 0, 0, 0, 1]);
    }
}
