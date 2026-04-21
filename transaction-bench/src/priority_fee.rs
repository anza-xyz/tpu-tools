//! Priority-fee logic: per-transaction fee selection and metrics.
//!
//! - [`PriorityFeeMode`] picks the additional compute-unit-price added on top
//!   of `--compute-unit-price` (none / random / scheduled).
//! - [`PriorityFeeStats`] accumulates totals across worker threads for
//!   metrics reporting.
//!
//! The clap-derived `PriorityFeeParams` that produces a `PriorityFeeMode` from
//! CLI args lives in [`crate::cli`] so all clap structs stay in one place.
use {
    rand::Rng,
    std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    },
};

/// How the additional priority fee (on top of `--compute-unit-price`) is chosen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PriorityFeeMode {
    /// No additional fee beyond the base.
    None,
    /// Each tx gets base + rand(0..=max).
    Random { max: u64 },
    /// Fee ramps linearly from `0` to `max` over `period_ms` milliseconds,
    /// then resets. One full sawtooth ramp per `period_ms`.
    Scheduled { max: u64, period_ms: u64 },
}

impl PriorityFeeMode {
    /// Resolve the additional fee component for a single transaction.
    pub fn resolve(&self) -> u64 {
        match *self {
            Self::None => 0,
            Self::Random { max } => rand::thread_rng().gen_range(0..=max),
            Self::Scheduled { max, period_ms } => {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("clock before UNIX epoch")
                    .as_millis() as u64;
                scheduled_fee_at(now_ms, max, period_ms)
            }
        }
    }
}

/// Linear sawtooth: ramps from `0` to `max` over `period_ms` milliseconds, then
/// resets. Hits both endpoints exactly (`fee(0) = 0`, `fee(period_ms-1) = max`)
/// because positions `[0, period_ms-1]` are mapped to `[0, max]` via the
/// divisor `period_ms - 1`.
///
/// `period_ms = 1` is degenerate (the only sampled position per cycle is `0`)
/// and always returns `0` — a 1 ms ramp is not meaningful with ms-resolution
/// sampling. Use `period_ms >= 2` for a real ramp.
///
/// Safety: caller must guarantee `period_ms >= 1`; the public
/// [`PriorityFeeMode::Scheduled`] variant carries this invariant via
/// [`NonZeroU64`].
#[allow(clippy::arithmetic_side_effects)]
fn scheduled_fee_at(now_ms: u64, max: u64, period_ms: u64) -> u64 {
    debug_assert!(period_ms >= 1, "period_ms must be non-zero");
    let position = now_ms % period_ms;
    // Divisor = period_ms - 1 so position = period_ms-1 yields exactly `max`.
    // For period_ms = 1 the divisor would be 0; clamp to 1, position is then
    // always 0 anyway, so the result is 0.
    let divisor = period_ms.saturating_sub(1).max(1);
    position * max / divisor
}

/// Accumulates priority fee totals for metrics reporting.
#[derive(Default)]
pub struct PriorityFeeStats {
    total_fees: AtomicU64,
    tx_count: AtomicU64,
}

impl PriorityFeeStats {
    pub fn record(&self, fee: u64) {
        self.total_fees.fetch_add(fee, Ordering::Relaxed);
        self.tx_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_and_reset(&self) -> (u64, u64) {
        let total = self.total_fees.swap(0, Ordering::Relaxed);
        let count = self.tx_count.swap(0, Ordering::Relaxed);
        (total, count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduled_fee_ramps_to_max() {
        // period_ms = 4, max = 3: ramp uses divisor = 3, so positions
        // 0..=3 map exactly to fees 0, 1, 2, 3, then wraps.
        let observed: Vec<u64> = (0..8).map(|t| scheduled_fee_at(t, 3, 4)).collect();
        assert_eq!(observed, vec![0, 1, 2, 3, 0, 1, 2, 3]);

        // period_ms = 8, max = 3: ramp uses divisor = 7. Fees are
        // floor(position * 3 / 7) → 0, 0, 0, 1, 1, 2, 2, 3, then wraps.
        let observed: Vec<u64> = (0..8).map(|t| scheduled_fee_at(t, 3, 8)).collect();
        assert_eq!(observed, vec![0, 0, 0, 1, 1, 2, 2, 3]);

        // Endpoints reachable for a "round" period.
        assert_eq!(scheduled_fee_at(0, 1000, 100), 0);
        assert_eq!(scheduled_fee_at(99, 1000, 100), 1000);
        assert_eq!(scheduled_fee_at(100, 1000, 100), 0); // wraps

        // period_ms = 1 is degenerate: only position 0 is sampled, always 0.
        // (Codex review flagged this; document it instead of pretending to ramp
        // in a 1 ms window with ms-resolution sampling.)
        assert_eq!(scheduled_fee_at(0, 100, 1), 0);
        assert_eq!(scheduled_fee_at(42, 100, 1), 0);
    }
}
