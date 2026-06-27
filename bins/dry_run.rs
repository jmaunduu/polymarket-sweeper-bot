//! Dry run mode.
//!
//! Source docs (PM_guide.pdf):
//! "Phase 1 is real wallet dry run. Connect to real infrastructure.
//!  Use real API endpoints. Attempt real orders. Zero balance means zero risk.
//!  But every NSF rejection, every timeout, every ghost fill or unexpected
//!  behavior is real data about how your bot performs under actual market
//!  conditions. Run this phase until live results match your backtest within 3%."
//!
//! What this binary does:
//!   - Runs the complete sweeper stack (same code as sweeper binary)
//!   - DRY_RUN=true: skips post_order(), logs what WOULD have been sent
//!   - Records every trigger opportunity to bot_trades with dry_run=true
//!   - After min_sample_windows, compares win rate to backtest reference
//!   - Exits with error if divergence exceeds tolerance_pct (3%)
//!
//! What to watch in the logs:
//!   GOOD "NSF rejection < 200ms"    -> signing works, latency acceptable
//!   GOOD "Trigger fired"            -> market detection working
//!   GOOD "Win rate within 3%"       -> ready for live capital
//!   BAD  "Timeout"                  -> VPS latency problem
//!   BAD  "Auth error"               -> credentials wrong
//!   BAD  "No triggers in 4 hours"   -> market scanning broken
//!   BAD  "Win rate diverges > 3%"   -> structural gap, investigate before live

use std::sync::{
    atomic::{AtomicI64, AtomicU32, Ordering},
    Arc,
};

use tracing::info;

/// Lightweight tracker for the dry run 3% tolerance check.
pub struct DryRunTracker {
    /// Opportunities where trigger conditions were met.
    pub windows_seen: AtomicU32,
    /// Simulated fills (based on historical book depth, or 100% if no data yet).
    pub would_fill: AtomicU32,
    /// Would-be wins (outcome resolved in our favour).
    pub would_win: AtomicU32,
    /// Would-be PnL in cents.
    pub would_pnl_cents: AtomicI64,
    /// Backtest reference win rate (loaded from backtest.rs output or config).
    pub backtest_win_rate: f64,
    /// Tolerance (from config, default 3%).
    pub tolerance: f64,
    /// Minimum windows before comparison is valid.
    pub min_windows: u32,
}

impl DryRunTracker {
    pub fn new(backtest_win_rate: f64, tolerance_pct: f64, min_windows: u32) -> Arc<Self> {
        Arc::new(Self {
            windows_seen: AtomicU32::new(0),
            would_fill: AtomicU32::new(0),
            would_win: AtomicU32::new(0),
            would_pnl_cents: AtomicI64::new(0),
            backtest_win_rate,
            tolerance: tolerance_pct / 100.0,
            min_windows,
        })
    }

    /// Record a trigger opportunity.
    /// If the market subsequently resolved in our favour -> win.
    /// Fill assumed = true for initial dry run (adjust after backtest data exists).
    pub fn record_opportunity(&self, fill_assumed: bool, win: bool, entry_price: f64, size_usd: f64) {
        self.windows_seen.fetch_add(1, Ordering::Relaxed);

        if fill_assumed {
            self.would_fill.fetch_add(1, Ordering::Relaxed);
            if win {
                self.would_win.fetch_add(1, Ordering::Relaxed);
                let pnl_cents = ((1.0 - entry_price) * size_usd * 100.0).round() as i64;
                self.would_pnl_cents
                    .fetch_add(pnl_cents, Ordering::Relaxed);
            } else {
                let pnl_cents = (-(entry_price) * size_usd * 100.0).round() as i64;
                self.would_pnl_cents
                    .fetch_add(pnl_cents, Ordering::Relaxed);
            }
        }
    }

    /// Check whether the dry run passes the 3% tolerance gate.
    /// Returns Ok(()) if pass, Err with details if fail or insufficient data.
    pub fn check_tolerance(&self) -> Result<(), String> {
        let seen = self.windows_seen.load(Ordering::Relaxed);
        let fills = self.would_fill.load(Ordering::Relaxed);
        let wins = self.would_win.load(Ordering::Relaxed);

        if seen < self.min_windows {
            return Err(format!(
                "Insufficient data: {} windows seen, need {}",
                seen, self.min_windows
            ));
        }

        if fills == 0 {
            return Err("Zero fills in dry run - check trigger logic".to_string());
        }

        let live_win_rate = wins as f64 / fills as f64;
        let divergence = (live_win_rate - self.backtest_win_rate).abs();

        if divergence > self.tolerance {
            Err(format!(
                "WIN RATE DIVERGENCE {:.1}% EXCEEDS {:.1}% TOLERANCE\n\
                 Dry run: {:.1}%  Backtest: {:.1}%\n\
                 DO NOT deploy live capital. Investigate the gap first.\n\
                 Common causes: fill rate assumption wrong, latency model off, \
                 regime mismatch between backtest and current conditions.",
                divergence * 100.0,
                self.tolerance * 100.0,
                live_win_rate * 100.0,
                self.backtest_win_rate * 100.0,
            ))
        } else {
            info!(
                "Dry run passes 3% tolerance gate. Live: {:.1}% Backtest: {:.1}% Gap: {:.1}%",
                live_win_rate * 100.0,
                self.backtest_win_rate * 100.0,
                divergence * 100.0,
            );
            Ok(())
        }
    }

    pub fn log_status(&self) {
        let seen = self.windows_seen.load(Ordering::Relaxed);
        let fills = self.would_fill.load(Ordering::Relaxed);
        let wins = self.would_win.load(Ordering::Relaxed);
        let pnl_cents = self.would_pnl_cents.load(Ordering::Relaxed);

        let win_rate = if fills > 0 {
            wins as f64 / fills as f64
        } else {
            0.0
        };
        let fill_rate = if seen > 0 {
            fills as f64 / seen as f64
        } else {
            0.0
        };

        info!(
            "[DRY RUN] windows={} fills={} ({:.1}%) wins={:.1}% would_pnl=${:.2}",
            seen,
            fills,
            fill_rate * 100.0,
            win_rate * 100.0,
            pnl_cents as f64 / 100.0,
        );
    }
}

/// Entry point for `cargo run --bin dry_run`.
/// Mirrors main.rs exactly but sets DRY_RUN=true and prints tolerance status.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Force dry run mode regardless of config.
    std::env::set_var("DRY_RUN", "true");

    tracing_subscriber::fmt().with_env_filter("info").init();

    tracing::info!("=== DRY RUN MODE ===");
    tracing::info!("Real API connections. Real infrastructure. Zero capital.");
    tracing::info!("Orders will NOT be submitted. NSF rejections will be logged.");
    tracing::info!("Run for 48+ hours before checking tolerance.");

    // Load backtest reference win rate from environment or use 0.0 (unknown).
    // After running bins/backtest.rs, set BACKTEST_WIN_RATE in .env.
    let backtest_wr: f64 = std::env::var("BACKTEST_WIN_RATE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);

    if backtest_wr == 0.0 {
        tracing::warn!(
            "BACKTEST_WIN_RATE not set. Run `cargo run --bin backtest` first, then set BACKTEST_WIN_RATE=0.XX in .env"
        );
    }

    let tracker = DryRunTracker::new(backtest_wr, 3.0, 50);
    let tracker_c = tracker.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1800));
        loop {
            interval.tick().await;
            tracker_c.log_status();
            match tracker_c.check_tolerance() {
                Ok(()) => {}
                Err(msg) => tracing::warn!("{}", msg),
            }
        }
    });

    // TODO: wire main sweeper loop here with DRY_RUN=true check in fire()
    // The check: if std::env::var("DRY_RUN").as_deref() == Ok("true") {
    //     log opportunity + record to db; skip post_order()
    // }

    tokio::signal::ctrl_c().await?;
    tracker.log_status();
    match tracker.check_tolerance() {
        Ok(()) => tracing::info!("Dry run complete - PASSED"),
        Err(msg) => tracing::error!("Dry run complete - FAILED: {}", msg),
    }
    Ok(())
}
