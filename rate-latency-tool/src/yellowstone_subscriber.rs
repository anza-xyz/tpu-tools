use {
    crate::csv_writer::CSVRecord,
    futures::stream::StreamExt,
    log::{debug, error, info, trace, warn},
    solana_clock::Slot,
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
            subscribe_update::UpdateOneof, CommitmentLevel, SlotStatus, SubscribeRequest,
            SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions, SubscribeUpdateTransaction,
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

fn build_request(
    payers_pubkeys: &[Pubkey],
    subscribe_to_block_meta: bool,
    check_all_transactions: bool,
) -> SubscribeRequest {
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
        slots.insert(
            client_name.clone(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        );
        entry.insert(client_name.clone(), Default::default());
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

    let mut blocks_meta: HashMap<String, SubscribeRequestFilterBlocksMeta> = HashMap::new();
    if subscribe_to_block_meta {
        blocks_meta.insert(client_name.clone(), SubscribeRequestFilterBlocksMeta {});
    }

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

pub async fn run_yellowstone_subscriber_filtered(
    yellowstone_url: &str,
    yellowstone_token: Option<&str>,
    account_pubkeys: &[Pubkey],
    csv_sender: Sender<CSVRecord>,
    num_txs_per_block_sender: Option<Sender<(Slot, usize)>>,
    cancel: CancellationToken,
) -> Result<(), YellowstoneError> {
    let client_config = create_client_config(yellowstone_url, yellowstone_token);
    let mut client = create_geyser_client(client_config).await?;

    let request = build_request(account_pubkeys, num_txs_per_block_sender.is_some(), false);

    let (_subscribe_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;

    let main_loop = async move {
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
                        UpdateOneof::Transaction(msg) => {
                            debug!("Received transaction info: {msg:?}");
                            if let Ok(record) = try_build_csv_record(
                                msg,
                                received_timestamp,
                                received_subscr_timestamp,
                            ) {
                                if csv_sender.send(record).await.is_err() {
                                    info!(
                                        "Unexpectidly dropped CSVRecord receiver, stopping \
                                         yellowstone subscriber."
                                    );
                                    break;
                                }
                            } else {
                                warn!("Transaction update without transaction data.");
                            }
                        }
                        UpdateOneof::BlockMeta(msg) => {
                            if let Some(sender) = &num_txs_per_block_sender {
                                sender
                                    .send((msg.slot, msg.executed_transaction_count as usize))
                                    .await
                                    .map_err(|e| {
                                        error!(
                                            "Failed to send number of transactions per block: \
                                             {e:?}"
                                        );
                                        YellowstoneError::UnexpectedError
                                    })?;
                            }
                        }
                        _ => {
                            warn!("Unexpected message type in the update: {:?}.", msg);
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

pub async fn run_yellowstone_subscriber_all_txs(
    yellowstone_url: &str,
    yellowstone_token: Option<&str>,
    account_pubkeys: &[Pubkey],
    block_tx_sender: Sender<CSVRecord>,
    rpc_client: Arc<RpcClient>,
    cancel: CancellationToken,
) -> Result<(), YellowstoneError> {
    info!("run_yellowstone_subscriber_all_txs");
    let target_accounts: Arc<HashSet<Pubkey>> = Arc::new(account_pubkeys.iter().cloned().collect());
    let jito_tip_accounts = Arc::new(get_jito_tip_accounts(rpc_client).await);
    let amms_programs = Arc::new(prop_amms_programs());

    let client_config = create_client_config(yellowstone_url, yellowstone_token);
    let mut client = create_geyser_client(client_config).await?;

    let request = build_request(account_pubkeys, true, true);

    let mut stream = client.subscribe_once(request).await?;

    let main_loop = async move {
        let mut recording_map = HashMap::<u64, SlotRecording>::default();
        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => {
                    //let received_subscr_timestamp: Option<u64> = msg
                    //    .created_at
                    //    .and_then(|ts| ts.try_into().ok())
                    //    .and_then(|sys_time: SystemTime| sys_time.duration_since(UNIX_EPOCH).ok())
                    //    .map(|duration| duration.as_millis() as u64);
                    //let received_timestamp = Some(timestamp());
                    let Some(msg) = msg.update_oneof else {
                        warn!("Update not found in the message.");
                        continue;
                    };
                    match msg {
                        UpdateOneof::Transaction(tx) => {
                            trace!("Received transaction info: {tx:?}");
                            if let Some(recording) = recording_map.get_mut(&tx.slot) {
                                debug!("Adding tx to recording for slot {}", tx.slot);
                                recording.add_transaction(tx);
                            } else {
                                // this happens because we start tool between slot updates.
                                debug!("skipping txs for slot {}", tx.slot);
                            }
                        }
                        UpdateOneof::Entry(entry_update) => {
                            debug!("Received entry info: {entry_update:?}");
                            if let Some(recording) = recording_map.get_mut(&entry_update.slot) {
                                recording.add_entry_update(EntryRecord {
                                    index: entry_update.index,
                                    executed_transaction_count: entry_update
                                        .executed_transaction_count,
                                });
                            } else {
                                // this happens because we start tool between slot updates.
                                debug!("skipping entries for slot {}", entry_update.slot);
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
                                    // this is handled in BlockMeta
                                }
                                SlotStatus::SlotFirstShredReceived
                                | SlotStatus::SlotCreatedBank => {
                                    info!("Create recording for slot {}", grpc_slot.slot);
                                    recording_map.entry(grpc_slot.slot).or_insert_with(|| {
                                        SlotRecording {
                                            slot: grpc_slot.slot,
                                            txs: Vec::new(),
                                            entries: Vec::new(),
                                        }
                                    });
                                }
                                SlotStatus::SlotDead => {
                                    debug!("Removing recording for dead slot {}", grpc_slot.slot);
                                    let _ = recording_map.remove(&grpc_slot.slot);
                                }
                                _ => {}
                            }
                        }
                        UpdateOneof::BlockMeta(block_meta) => {
                            let executed_transaction_count = block_meta.executed_transaction_count;
                            let entry_count = block_meta.entries_count;
                            let slot = block_meta.slot;

                            debug!("Received block meta for slot {slot}, {executed_transaction_count}, {entry_count}" );
                            if let Some(recording) = recording_map.get_mut(&slot) {
                                debug!(
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
                                        debug!("Send to write");
                                        for tx in recording.txs {
                                            let tx = try_build_csv_record_with_classification(
                                                tx,
                                                None,
                                                None,
                                                &target_accounts,
                                                &jito_tip_accounts,
                                                &amms_programs,
                                            );
                                            if let Ok(mut tx) = tx {
                                                let tick = ticks
                                                    .get(tx.index_in_block.unwrap_or(0) as usize)
                                                    .cloned();
                                                tx.tick = tick;
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
                            warn!("Received unsupported update type.");
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

fn try_build_csv_record(
    msg: SubscribeUpdateTransaction,
    received_timestamp: Option<u64>,
    received_subscr_timestamp: Option<u64>,
) -> Result<CSVRecord, YellowstoneError> {
    if let Some(tx) = msg.transaction {
        if let Some(meta) = tx.meta {
            let memo_log = extract_memo_line(&meta.log_messages);
            if let Some(memo_log) = memo_log {
                if let Some((transaction_id, sent_slot, sent_timestamp)) = parse_memo_log(memo_log)
                {
                    //TODO(klykov): fix later unwraps
                    let signature = Signature::try_from(tx.signature.as_slice())
                        .unwrap()
                        .to_string();
                    let received_slot = msg.slot;
                    let index_in_block = Some(tx.index);

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
                        tx_type: None,
                        num_transaction_in_block: None,
                    });
                }
            }
        }
    }
    Err(YellowstoneError::EmptyTransactionUpdate)
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

struct SlotRecording {
    slot: u64,
    txs: Vec<SubscribeUpdateTransaction>,
    entries: Vec<EntryRecord>,
}

#[derive(Clone, Debug, Default)]
struct EntryRecord {
    index: u64,
    executed_transaction_count: u64,
}

impl SlotRecording {
    fn add_transaction(&mut self, tx: SubscribeUpdateTransaction) {
        self.txs.push(tx);
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
        self.txs
            .sort_by_key(|tx| tx.transaction.as_ref().map(|t| t.index).unwrap_or(0));
        self.entries.sort_by_key(|e| e.index);

        let mut total_executed_txn_count = 0u64;
        let mut begin = 0;
        let mut num_0_ticks = 0;
        let mut tx_id = 0;
        let mut ticks = vec![0u64; self.txs.len()];
        for entry in self.entries.iter() {
            // tick entry are entries with zero executed_transaction_count
            if entry.executed_transaction_count == 0 {
                let relevant_txs = &mut self.txs[begin..begin + total_executed_txn_count as usize];

                for _tx in relevant_txs {
                    ticks[tx_id] = num_0_ticks;
                    tx_id += 1;
                }

                begin += total_executed_txn_count as usize;
                total_executed_txn_count = 0;
                num_0_ticks += 1;
            } else {
                total_executed_txn_count += entry.executed_transaction_count;
            }
        }
        ticks
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
    msg: SubscribeUpdateTransaction,
    received_timestamp: Option<u64>,
    received_subscr_timestamp: Option<u64>,
    target_accounts: &Arc<HashSet<Pubkey>>,
    jito_tip_accounts: &Arc<HashSet<Pubkey>>,
    amms_programs: &Arc<HashMap<Pubkey, PropAMM>>,
) -> Result<CSVRecord, YellowstoneError> {
    let Some(tx) = msg.transaction else {
        return Err(YellowstoneError::EmptyTransactionUpdate);
    };
    let Some(meta) = tx.meta else {
        return Err(YellowstoneError::EmptyTransactionUpdate);
    };
    let signature = Signature::try_from(tx.signature.as_slice())
        .unwrap()
        .to_string();
    let received_slot = msg.slot;
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
