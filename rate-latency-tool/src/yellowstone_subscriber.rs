use {
    crate::csv_writer::CSVRecord,
    futures::stream::StreamExt,
    log::{debug, error, info, warn},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_time_utils::timestamp,
    std::{
        collections::HashMap,
        io,
        path::PathBuf,
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
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
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
        .x_token(x_token)?
        .tls_config(tls_config)?
        .max_decoding_message_size(max_decoding_message_size)
        .connect_timeout(timeout)
        .keep_alive_timeout(timeout)
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
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
        max_decoding_message_size: 16 * 1024 * 1024,
        timeout: Duration::from_secs(30), // 30 seconds
    }
}

fn build_request(payers_pubkeys: &[Pubkey]) -> SubscribeRequest {
    let mut transactions = HashMap::<String, SubscribeRequestFilterTransactions>::new();
    transactions.insert(
        "client".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: payers_pubkeys.iter().map(|pk| pk.to_string()).collect(),
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions,
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
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
    csv_sender: Sender<CSVRecord>,
    cancel: CancellationToken,
) -> Result<(), YellowstoneError> {
    let client_config = create_client_config(yellowstone_url, yellowstone_token);
    let mut client = create_geyser_client(client_config).await?;

    let request = build_request(account_pubkeys);

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
                    match msg.update_oneof {
                        Some(UpdateOneof::Transaction(msg)) => {
                            debug!("Received transaction info: {msg:?}");
                            if let Ok(record) = try_build_csv_record(
                                msg,
                                received_timestamp,
                                received_subscr_timestamp,
                            ) {
                                if csv_sender.send(record).await.is_err() {
                                    info!("Unexpectidly dropped CSVRecord receiver, stopping yellowstone subscriber.");
                                    break;
                                }
                            } else {
                                warn!("Transaction update without transaction data.");
                            }
                        }
                        Some(_) => {
                            warn!(
                                "Unexpected message type in the update: {:?}.",
                                msg.update_oneof
                            );
                        }
                        None => {
                            warn!("Update not found in the message.");
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

                    return Ok(CSVRecord {
                        signature,
                        transaction_id,
                        sent_slot,
                        received_slot: Some(received_slot),
                        sent_timestamp,
                        received_timestamp,
                        received_subscr_timestamp,
                        tx_status: vec![],
                    });
                }
            }
        }
    }
    Err(YellowstoneError::EmptyTransactionUpdate)
}

/// Extracts log line which starts with `Program log: Memo` from given log
/// lines.
fn extract_memo_line<'a, T: AsRef<str>>(logs: &'a [T]) -> Option<&'a str> {
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
