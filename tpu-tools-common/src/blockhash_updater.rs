//! Background blockhash refresh for transaction generators.
//!
//! The updater polls RPC and publishes blockhashes through a [`tokio::sync::watch`]
//! channel. By default it publishes the freshest blockhash. When configured with a
//! non-zero `stale_secs`, it instead publishes the blockhash that was latest that long
//! ago. One use case is creating already-expired transactions to exercise the scheduler's
//! discard-on-age path.

use {
    log::*,
    solana_hash::Hash,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    std::{collections::VecDeque, sync::Arc},
    thiserror::Error,
    tokio::{
        sync::watch,
        time::{self, Duration, Instant},
    },
};

/// Config was introduced for test purposes.
#[derive(Clone, Copy, Debug)]
struct BlockhashUpdaterConfig {
    /// How often request the new blockhash.
    update_interval: Duration,
    /// If we fail to update blockhash for this period of time, we give up and report error.
    stuck_interval: Duration,
    /// When start to report warnings about blockhash not updating.
    not_updating_interval: Duration,
    /// How often to report blockhash errors if any.
    error_report_interval: Duration,
}

impl Default for BlockhashUpdaterConfig {
    fn default() -> Self {
        Self {
            update_interval: Duration::from_secs(1),
            stuck_interval: Duration::from_secs(120),
            not_updating_interval: Duration::from_secs(30),
            error_report_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Error, Debug, PartialEq, Eq, Clone, Copy)]
pub enum BlockhashUpdaterError {
    /// The blockhash did not change within the configured stuck interval.
    #[error("Blockhash is stuck.")]
    BlockhashStuck,
}

/// How the updater turns fetched blockhashes into published ones.
enum Strategy {
    /// Publish the freshest blockhash as observed.
    Fresh,
    /// Publish the blockhash that was latest roughly `stale_for` ago, from a delay
    /// line of observed blockhashes, so the published blockhash stays a constant
    /// age as the tip advances.
    DelayLine {
        stale_for: Duration,
        history: VecDeque<(Instant, Hash)>,
    },
}

impl Strategy {
    /// Digests a fetched blockhash and returns the hash to publish (`None` when
    /// there is nothing new relative to `last`) along with whether this
    /// observation counts as progress for stuck detection. Both strategies make
    /// progress only when the observed tip advances, so a chain that stops
    /// producing blockhashes is reported stuck even while RPC keeps responding.
    /// For `DelayLine` the tip is tracked by the history, not by `last`, whose
    /// published output deliberately lags.
    fn observe(&mut self, now: Instant, fetched: Hash, last: Hash) -> (Option<Hash>, bool) {
        match self {
            Self::Fresh => {
                let changed = fetched != last;
                (changed.then_some(fetched), changed)
            }
            Self::DelayLine { stale_for, history } => {
                let tip_advanced = history.back().map(|(_, h)| *h) != Some(fetched);
                if tip_advanced {
                    history.push_back((now, fetched));
                }
                prune_history(history, now, *stale_for);
                let selected =
                    select_aged_blockhash(history, now, *stale_for).filter(|hash| *hash != last);
                (selected, tip_advanced)
            }
        }
    }
}

/// Polls RPC for blockhashes and publishes them to a watch channel, selecting what to
/// publish according to a [`Strategy`].
///
/// The updater exits when all receivers for the watch channel have been dropped, or returns
/// [`BlockhashUpdaterError::BlockhashStuck`] if RPC keeps failing (or returns the same
/// blockhash) for too long.
pub struct BlockhashUpdater {
    rpc_client: Arc<RpcClient>,
    sender: watch::Sender<Hash>,
    config: BlockhashUpdaterConfig,
    last_blockhash: Hash,
    strategy: Strategy,
}

impl BlockhashUpdater {
    /// Creates a blockhash updater that always publishes the freshest blockhash.
    pub fn new(rpc_client: Arc<RpcClient>, sender: watch::Sender<Hash>) -> Self {
        Self {
            rpc_client,
            sender,
            config: BlockhashUpdaterConfig::default(),
            last_blockhash: Hash::default(),
            strategy: Strategy::Fresh,
        }
    }

    /// Creates a delay-line updater that publishes the blockhash that was latest roughly
    /// `stale_secs` ago. It uses only `getLatestBlockhash`, so it needs nothing special on
    /// the target RPC (no `getBlock` / `--enable-rpc-transaction-history`). The published
    /// staleness ramps up from fresh over the first `stale_secs` of observation (warmup);
    /// callers should prime for `stale_secs` before relying on it.
    pub fn with_stale_secs(
        rpc_client: Arc<RpcClient>,
        sender: watch::Sender<Hash>,
        stale_secs: Duration,
    ) -> Self {
        Self {
            rpc_client,
            sender,
            config: BlockhashUpdaterConfig::default(),
            last_blockhash: Hash::default(),
            strategy: Strategy::DelayLine {
                stale_for: stale_secs,
                history: VecDeque::new(),
            },
        }
    }

    #[cfg(test)]
    fn with_config(
        rpc_client: Arc<RpcClient>,
        sender: watch::Sender<Hash>,
        config: BlockhashUpdaterConfig,
    ) -> Self {
        Self {
            rpc_client,
            sender,
            config,
            last_blockhash: Hash::default(),
            strategy: Strategy::Fresh,
        }
    }

    /// Runs the updater until the watch channel is closed or the blockhash is
    /// considered stuck.
    pub async fn run(mut self) -> Result<(), BlockhashUpdaterError> {
        let mut blockhash_last_updated = Instant::now();
        let mut last_error_log = Instant::now();
        let mut interval = time::interval(self.config.update_interval);
        while !self.sender.is_closed() {
            interval.tick().await;
            let now = Instant::now();

            if let Ok(fetched) = self.rpc_client.get_latest_blockhash().await {
                let (publish, progress) = self.strategy.observe(now, fetched, self.last_blockhash);
                if let Some(hash) = publish {
                    self.last_blockhash = hash;
                    if self.sender.send(hash).is_err() {
                        break;
                    }
                }
                if progress {
                    blockhash_last_updated = now;
                }
            }

            if blockhash_last_updated.elapsed() > self.config.stuck_interval {
                return Err(BlockhashUpdaterError::BlockhashStuck);
            } else if blockhash_last_updated.elapsed() > self.config.not_updating_interval
                && last_error_log.elapsed() >= self.config.error_report_interval
            {
                last_error_log = Instant::now();
                let last_updated_s = blockhash_last_updated.elapsed().as_secs();
                warn!("Blockhash is not updating for {last_updated_s} s.");
            }
        }
        Ok(())
    }
}

/// Returns the blockhash that was latest approximately `stale_for` ago: the newest history
/// entry whose age is at least `stale_for`, or — during warmup, before any entry is that old —
/// the oldest entry, so the published age ramps up toward `stale_for`.
fn select_aged_blockhash(
    history: &VecDeque<(Instant, Hash)>,
    now: Instant,
    stale_for: Duration,
) -> Option<Hash> {
    history
        .iter()
        .rev()
        .find(|(t, _)| now.saturating_duration_since(*t) >= stale_for)
        .or_else(|| history.front())
        .map(|(_, h)| *h)
}

/// Drops history entries older than the one in effect `stale_for` ago, keeping at least one.
fn prune_history(history: &mut VecDeque<(Instant, Hash)>, now: Instant, stale_for: Duration) {
    while history.len() > 1 && now.saturating_duration_since(history[1].0) >= stale_for {
        history.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        serde_json::{self, json},
        solana_rpc_client::mock_sender::PUBKEY,
        solana_rpc_client_api::{
            request::RpcRequest,
            response::{Response, RpcBlockhash, RpcResponseContext},
        },
        solana_sha256_hasher::hash,
        std::collections::HashMap,
        tokio::{sync::watch, task, time::sleep},
    };

    // Lower than default values to avoid long running unit tests.
    fn test_config() -> BlockhashUpdaterConfig {
        BlockhashUpdaterConfig {
            update_interval: Duration::from_millis(200),
            stuck_interval: Duration::from_millis(600),
            not_updating_interval: Duration::from_millis(300),
            error_report_interval: Duration::from_millis(1000),
        }
    }

    // `Instant + Duration` trips `clippy::arithmetic_side_effects`; go through `checked_add`.
    fn at(base: Instant, secs: u64) -> Instant {
        base.checked_add(Duration::from_secs(secs))
            .expect("instant within range")
    }

    fn make_history(base: Instant, count: u8) -> (VecDeque<(Instant, Hash)>, Vec<Hash>) {
        let hashes: Vec<Hash> = (0..count).map(|i| hash(&[i])).collect();
        let history = hashes
            .iter()
            .enumerate()
            .map(|(i, h)| (at(base, i as u64), *h))
            .collect();
        (history, hashes)
    }

    #[test]
    fn test_select_aged_blockhash_steady_state() {
        let base = Instant::now();
        let (history, hashes) = make_history(base, 10); // entries at base+0s..base+9s
        let now = at(base, 9);
        // 5s of staleness at t=9s selects the blockhash that was latest at t=4s.
        assert_eq!(
            select_aged_blockhash(&history, now, Duration::from_secs(5)),
            Some(hashes[4])
        );
    }

    #[test]
    fn test_select_aged_blockhash_warmup_falls_back_to_oldest() {
        let base = Instant::now();
        let (history, hashes) = make_history(base, 2); // only 2s of history
        let now = at(base, 2);
        // Want 5s but only have 2s: ramp by returning the oldest observed blockhash.
        assert_eq!(
            select_aged_blockhash(&history, now, Duration::from_secs(5)),
            Some(hashes[0])
        );
    }

    #[test]
    fn test_select_aged_blockhash_empty() {
        let history = VecDeque::new();
        assert_eq!(
            select_aged_blockhash(&history, Instant::now(), Duration::from_secs(5)),
            None
        );
    }

    #[test]
    fn test_prune_history_keeps_selectable_window() {
        let base = Instant::now();
        let (mut history, hashes) = make_history(base, 10);
        let now = at(base, 9);
        prune_history(&mut history, now, Duration::from_secs(5));
        // The entry latest 5s ago (t=4s) survives as the new front, and pruning does not
        // change which blockhash gets selected.
        assert_eq!(history.front().map(|(_, h)| *h), Some(hashes[4]));
        assert_eq!(
            select_aged_blockhash(&history, now, Duration::from_secs(5)),
            Some(hashes[4])
        );
    }

    #[test]
    fn test_fresh_strategy_observe() {
        let mut strategy = Strategy::Fresh;
        let now = Instant::now();
        let last = hash(&[0]);
        let fetched = hash(&[1]);
        // A changed blockhash is published and counts as progress.
        assert_eq!(strategy.observe(now, fetched, last), (Some(fetched), true));
        // An unchanged blockhash is neither published nor progress.
        assert_eq!(strategy.observe(now, fetched, fetched), (None, false));
    }

    #[test]
    fn test_delay_line_strategy_observe() {
        let base = Instant::now();
        let mut strategy = Strategy::DelayLine {
            stale_for: Duration::from_secs(5),
            history: VecDeque::new(),
        };
        let first = hash(&[0]);
        let second = hash(&[1]);
        // Warmup: the first observation is published (oldest-entry fallback).
        assert_eq!(
            strategy.observe(base, first, Hash::default()),
            (Some(first), true)
        );
        // Nothing new to publish while the delay line fills, but the advancing
        // tip counts as progress.
        assert_eq!(strategy.observe(at(base, 1), second, first), (None, true));
        // Once the second entry is stale_for old, it becomes the publish
        // candidate; an unchanged tip is no longer progress.
        assert_eq!(
            strategy.observe(at(base, 6), second, first),
            (Some(second), false)
        );
    }

    #[tokio::test]
    async fn test_blockhash_updates_successfully() {
        let rpc_blockhash = hash(&[1u8]);
        let mut mocks = HashMap::new();
        mocks.insert(
            RpcRequest::GetLatestBlockhash,
            json!(Response {
                context: RpcResponseContext {
                    slot: 1,
                    api_version: None
                },
                value: json!(RpcBlockhash {
                    blockhash: rpc_blockhash.to_string(),
                    last_valid_block_height: 42,
                }),
            }),
        );
        let rpc_client = Arc::new(RpcClient::new_mock_with_mocks("".to_string(), mocks));
        let (sender, receiver) = watch::channel(Hash::default());
        let updater_config = test_config();
        let updater = BlockhashUpdater::with_config(rpc_client, sender, updater_config);
        let handle = task::spawn(async move { updater.run().await });
        // sleep to let updater task entering the update loop.
        sleep(updater_config.update_interval / 2).await;
        let blockhash = *receiver.borrow();
        assert_eq!(rpc_blockhash, blockhash);
        drop(receiver);
        let result = handle.await.expect("task should not panic.");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_blockhash_updates_stuck() {
        let rpc_client = Arc::new(RpcClient::new_mock("fails".to_string()));
        let (sender, receiver) = watch::channel(Hash::default());
        let updater = BlockhashUpdater::with_config(rpc_client, sender, test_config());
        let handle = task::spawn(async move { updater.run().await });
        assert_eq!(*receiver.borrow(), Hash::default());
        let err = handle.await.expect("task should not panic.");
        assert_eq!(err, Err(BlockhashUpdaterError::BlockhashStuck));
    }

    #[tokio::test]
    async fn test_blockhash_updates_stuck_recover() {
        // MockRpcClient will first return specified response and later will always send
        // the same PUBKEY hash.
        let expected_blockhash: Hash = PUBKEY.parse().unwrap();
        let mut mocks = HashMap::new();
        mocks.insert(
            RpcRequest::GetLatestBlockhash,
            json!(Response {
                context: RpcResponseContext {
                    slot: 1,
                    api_version: None
                },
                value: serde_json::value::Value::Null,
            }),
        );
        let rpc_client = Arc::new(RpcClient::new_mock_with_mocks("".to_string(), mocks));
        let (sender, receiver) = watch::channel(Hash::default());
        let updater_config = test_config();
        let updater = BlockhashUpdater::with_config(rpc_client, sender, updater_config);
        let handle = task::spawn(async move { updater.run().await });
        // sleep to let updater task entering the update loop.
        sleep(updater_config.update_interval / 2).await;
        let blockhash = *receiver.borrow();
        assert_eq!(
            Hash::default(),
            blockhash,
            "Cannot update blockhash because rpc_client returns Null."
        );

        sleep(updater_config.update_interval).await;
        let blockhash = *receiver.borrow();
        assert_eq!(expected_blockhash, blockhash);
        drop(receiver);
        let result = handle.await.expect("task should not panic.");
        assert!(result.is_ok());
    }
}
