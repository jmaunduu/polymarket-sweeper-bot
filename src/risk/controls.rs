//! Risk controls and circuit breakers.
//!
//! This module centralizes daily drawdown limits, loss-streak pauses, fill
//! rate monitoring, ghost fill detection, and hot-path-readable metrics.

use crate::config::RiskConfig;
use crate::monitor::alerts::{AlertEvent, TelegramAlerter};
use atomic_float::AtomicF64;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering},
    Arc,
};

/// Checked before every order placement.
#[derive(Debug, Clone, PartialEq)]
pub enum RiskStatus {
    Clear,
    DailyLossCap(f64),
    ConsecutiveLossPause {
        losses: u32,
        windows_remaining: u32,
    },
    FillRateTooLow(f64),
    GhostFillsExceeded(u32),
    SessionDrawdownExceeded(f64),
}

/// Hot-path-readable metrics that avoid locking.
pub struct HotMetrics {
    pub daily_pnl_cents: AtomicI64,
    pub paused: AtomicBool,
    pub fill_attempts: AtomicU32,
    pub fill_successes: AtomicU32,
    pub session_peak_pnl: AtomicF64,
}

impl HotMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            daily_pnl_cents: AtomicI64::new(0),
            paused: AtomicBool::new(false),
            fill_attempts: AtomicU32::new(0),
            fill_successes: AtomicU32::new(0),
            session_peak_pnl: AtomicF64::new(0.0),
        })
    }

    pub fn daily_pnl(&self) -> f64 {
        self.daily_pnl_cents.load(Ordering::Relaxed) as f64 / 100.0
    }

    pub fn fill_rate(&self) -> f64 {
        let attempts = self.fill_attempts.load(Ordering::Relaxed);
        if attempts == 0 {
            return 1.0;
        }
        self.fill_successes.load(Ordering::Relaxed) as f64 / attempts as f64
    }

    pub fn session_drawdown_pct(&self) -> f64 {
        let peak = self.session_peak_pnl.load(Ordering::Relaxed);
        if peak <= 0.0 {
            return 0.0;
        }

        let current = self.daily_pnl();
        if current >= peak {
            0.0
        } else {
            (peak - current) / peak
        }
    }
}

struct SlowState {
    consecutive_losses: u32,
    pause_remaining: u32,
    ghost_fills_hour: Vec<DateTime<Utc>>,
    recent_outcomes: VecDeque<bool>,
}

pub struct RiskControls {
    config: RiskConfig,
    hot: Arc<HotMetrics>,
    slow: Arc<Mutex<SlowState>>,
    alerter: Arc<TelegramAlerter>,
}

impl RiskControls {
    pub fn new(config: RiskConfig, alerter: Arc<TelegramAlerter>) -> (Self, Arc<HotMetrics>) {
        let hot = HotMetrics::new();
        let controls = Self {
            config,
            hot: hot.clone(),
            slow: Arc::new(Mutex::new(SlowState {
                consecutive_losses: 0,
                pause_remaining: 0,
                ghost_fills_hour: Vec::new(),
                recent_outcomes: VecDeque::with_capacity(100),
            })),
            alerter,
        };

        (controls, hot)
    }

    /// Called before every order. Returns the first breached condition or Clear.
    pub fn check(&self) -> RiskStatus {
        let daily_pnl = self.hot.daily_pnl();
        if daily_pnl <= -self.config.max_daily_loss_usd {
            return RiskStatus::DailyLossCap(daily_pnl);
        }

        if self.hot.paused.load(Ordering::Relaxed) {
            let slow = self.slow.lock();
            if slow.pause_remaining > 0 {
                return RiskStatus::ConsecutiveLossPause {
                    losses: slow.consecutive_losses,
                    windows_remaining: slow.pause_remaining,
                };
            }

            self.hot.paused.store(false, Ordering::Relaxed);
        }

        let attempts = self.hot.fill_attempts.load(Ordering::Relaxed);
        if attempts >= 20 {
            let rate = self.hot.fill_rate();
            if rate < self.config.min_fill_rate {
                return RiskStatus::FillRateTooLow(rate);
            }
        }

        let session_drawdown = self.hot.session_drawdown_pct();
        if session_drawdown > self.config.max_session_drawdown_pct {
            return RiskStatus::SessionDrawdownExceeded(session_drawdown);
        }

        {
            let slow = self.slow.lock();
            let now = Utc::now();
            let recent_ghosts = slow
                .ghost_fills_hour
                .iter()
                .filter(|&&ts| (now - ts).num_seconds() < 3600)
                .count() as u32;

            if recent_ghosts >= self.config.max_ghost_fills_per_hour {
                return RiskStatus::GhostFillsExceeded(recent_ghosts);
            }
        }

        RiskStatus::Clear
    }

    /// Called when a window closes, whether or not the order filled.
    pub fn record_window(&self, filled: bool, win: bool, pnl_usd: f64) {
        self.hot.fill_attempts.fetch_add(1, Ordering::Relaxed);
        if filled {
            self.hot.fill_successes.fetch_add(1, Ordering::Relaxed);
        }

        if !filled {
            let mut slow = self.slow.lock();
            if slow.pause_remaining > 0 {
                slow.pause_remaining -= 1;
                if slow.pause_remaining == 0 {
                    self.hot.paused.store(false, Ordering::Relaxed);
                }
            }
            return;
        }

        let cents = (pnl_usd * 100.0).round() as i64;
        self.hot.daily_pnl_cents.fetch_add(cents, Ordering::Relaxed);

        let current_pnl = self.hot.daily_pnl();
        let peak_pnl = self.hot.session_peak_pnl.load(Ordering::Relaxed);
        if current_pnl > peak_pnl {
            self.hot.session_peak_pnl.store(current_pnl, Ordering::Relaxed);
        }

        let mut slow = self.slow.lock();
        if slow.recent_outcomes.len() >= 100 {
            slow.recent_outcomes.pop_front();
        }
        slow.recent_outcomes.push_back(win);

        if !win {
            slow.consecutive_losses += 1;
            if slow.consecutive_losses >= self.config.consecutive_loss_pause {
                slow.pause_remaining = self.config.pause_windows;
                self.hot.paused.store(true, Ordering::Relaxed);

                self.alerter.alert(AlertEvent::ConsecutiveLossPause {
                    streak: slow.consecutive_losses,
                    windows_paused: self.config.pause_windows,
                });

                tracing::warn!(
                    "Loss streak {}. Pausing {} windows.",
                    slow.consecutive_losses,
                    self.config.pause_windows
                );
            }
        } else {
            slow.consecutive_losses = 0;
        }

        let daily_pnl = self.hot.daily_pnl();
        if daily_pnl <= -self.config.max_daily_loss_usd {
            self.alerter.alert(AlertEvent::DailyLossCap { loss: daily_pnl });
            tracing::error!("Daily loss cap hit: ${:.2}. Bot paused.", daily_pnl);
        }
    }

    /// Record that a ghost fill was observed.
    pub fn record_ghost_fill(&self) {
        let mut slow = self.slow.lock();
        slow.ghost_fills_hour.push(Utc::now());

        let now = Utc::now();
        let recent = slow
            .ghost_fills_hour
            .iter()
            .filter(|&&ts| (now - ts).num_seconds() < 3600)
            .count() as u32;

        self.alerter
            .alert(AlertEvent::GhostFillDetected { count_this_hour: recent });
        tracing::error!("Ghost fill detected. Count this hour: {}", recent);
    }

    /// Reset state at UTC midnight.
    pub fn daily_reset(&self) {
        self.hot.daily_pnl_cents.store(0, Ordering::Relaxed);
        self.hot.fill_attempts.store(0, Ordering::Relaxed);
        self.hot.fill_successes.store(0, Ordering::Relaxed);
        self.hot.paused.store(false, Ordering::Relaxed);
        self.hot.session_peak_pnl.store(0.0, Ordering::Relaxed);

        let mut slow = self.slow.lock();
        slow.consecutive_losses = 0;
        slow.pause_remaining = 0;
        slow.ghost_fills_hour.clear();
        slow.recent_outcomes.clear();

        tracing::info!("Risk controls daily reset.");
    }

    /// Return current metrics for summaries and monitoring.
    pub fn snapshot(&self) -> RiskSnapshot {
        let slow = self.slow.lock();
        let now = Utc::now();

        RiskSnapshot {
            daily_pnl: self.hot.daily_pnl(),
            fill_rate: self.hot.fill_rate(),
            fill_attempts: self.hot.fill_attempts.load(Ordering::Relaxed),
            fill_successes: self.hot.fill_successes.load(Ordering::Relaxed),
            consecutive_losses: slow.consecutive_losses,
            paused: self.hot.paused.load(Ordering::Relaxed),
            ghost_fills_hour: slow
                .ghost_fills_hour
                .iter()
                .filter(|&&ts| (now - ts).num_seconds() < 3600)
                .count() as u32,
            recent_win_rate: {
                let wins = slow.recent_outcomes.iter().filter(|&&outcome| outcome).count();
                let count = slow.recent_outcomes.len();
                if count > 0 {
                    wins as f64 / count as f64
                } else {
                    0.0
                }
            },
            session_drawdown_pct: self.hot.session_drawdown_pct(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RiskSnapshot {
    pub daily_pnl: f64,
    pub fill_rate: f64,
    pub fill_attempts: u32,
    pub fill_successes: u32,
    pub consecutive_losses: u32,
    pub paused: bool,
    pub ghost_fills_hour: u32,
    pub recent_win_rate: f64,
    pub session_drawdown_pct: f64,
}
