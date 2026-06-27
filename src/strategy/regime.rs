//! Regime filtering and per-regime performance tracking.
//!
//! The strategy should at least separate weekday/weekend behavior and UTC-hour
//! buckets so real trading data can identify the most profitable regimes.

use crate::config::RegimeConfig;
use chrono::{Datelike, Timelike, Utc};
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};

/// Key used to group regime stats.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RegimeKey {
    pub is_weekend: bool,
    pub utc_hour: u32,
}

impl RegimeKey {
    pub fn current() -> Self {
        let now = Utc::now();
        Self {
            is_weekend: now.weekday().num_days_from_monday() >= 5,
            utc_hour: now.hour(),
        }
    }

    pub fn label(&self) -> String {
        let day = if self.is_weekend {
            "Weekend"
        } else {
            "Weekday"
        };
        format!("{} {:02}:00 UTC", day, self.utc_hour)
    }
}

/// Accumulated stats per regime key.
#[derive(Debug, Default, Clone)]
pub struct RegimeStats {
    pub fills: u32,
    pub wins: u32,
    pub attempts: u32,
    pub total_pnl: f64,
}

impl RegimeStats {
    pub fn win_rate(&self) -> f64 {
        if self.fills == 0 {
            0.0
        } else {
            self.wins as f64 / self.fills as f64
        }
    }

    pub fn fill_rate(&self) -> f64 {
        if self.attempts == 0 {
            0.0
        } else {
            self.fills as f64 / self.attempts as f64
        }
    }
}

/// Tracks per-regime performance stats.
pub struct RegimeTracker {
    stats: Arc<Mutex<HashMap<RegimeKey, RegimeStats>>>,
}

impl RegimeTracker {
    pub fn new() -> Self {
        Self {
            stats: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Record the outcome of a window attempt.
    pub fn record(&self, key: RegimeKey, filled: bool, win: bool, pnl: f64) {
        let mut map = self.stats.lock();
        let entry = map.entry(key).or_default();
        entry.attempts += 1;
        if filled {
            entry.fills += 1;
            if win {
                entry.wins += 1;
            }
            entry.total_pnl += pnl;
        }
    }

    /// Log a summary sorted by total PnL descending.
    pub fn log_summary(&self) {
        let map = self.stats.lock();
        if map.is_empty() {
            tracing::info!("Regime stats: no data yet.");
            return;
        }

        let mut entries: Vec<(&RegimeKey, &RegimeStats)> = map.iter().collect();
        entries.sort_by(|a, b| {
            b.1.total_pnl
                .partial_cmp(&a.1.total_pnl)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        tracing::info!("Regime performance summary:");
        for (key, stats) in &entries {
            tracing::info!(
                "{:25} fills={:3}/{:3} ({:4.1}%) wins={:4.1}% pnl=${:+.2}",
                key.label(),
                stats.fills,
                stats.attempts,
                stats.fill_rate() * 100.0,
                stats.win_rate() * 100.0,
                stats.total_pnl
            );
        }

        if let Some((best_key, best_stats)) = entries.first() {
            tracing::info!(
                "Best regime: {} (PnL ${:.2})",
                best_key.label(),
                best_stats.total_pnl
            );
        }
    }

    /// Return the label of the highest-PnL regime.
    pub fn best_regime_label(&self) -> String {
        let map = self.stats.lock();
        map.iter()
            .max_by(|a, b| {
                a.1.total_pnl
                    .partial_cmp(&b.1.total_pnl)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(key, _)| key.label())
            .unwrap_or_else(|| "none yet".to_string())
    }
}

impl Default for RegimeTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Gate that determines whether trading is allowed in the current regime.
pub struct RegimeFilter {
    config: RegimeConfig,
}

impl RegimeFilter {
    pub fn new(config: RegimeConfig) -> Self {
        Self { config }
    }

    /// Returns true if trading is allowed right now.
    pub fn is_allowed(&self) -> bool {
        let now = Utc::now();
        let hour = now.hour();
        let weekend = now.weekday().num_days_from_monday() >= 5;

        let day_ok = if weekend {
            self.config.weekend_enabled
        } else {
            self.config.weekday_enabled
        };
        if !day_ok {
            return false;
        }

        if !self.config.allowed_utc_hours.is_empty()
            && !self.config.allowed_utc_hours.contains(&hour)
        {
            return false;
        }

        true
    }
}
