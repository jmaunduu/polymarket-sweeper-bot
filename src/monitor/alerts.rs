//! Telegram alerts for circuit breaker events and daily summaries.
//!
//! Alerts should fire on unusual behavior such as drawdown thresholds, fill
//! rate deterioration, loss-streak pauses, and ghost fill detection.

use crate::config::TelegramConfig;
use anyhow::Result;
use reqwest::Client;
use serde_json::json;
use tracing::{error, info};

#[derive(Clone)]
pub struct TelegramAlerter {
    client: Client,
    token: String,
    chat_id: String,
    enabled: bool,
}

#[derive(Debug, Clone)]
pub enum AlertEvent {
    DailyLossCap {
        loss: f64,
    },
    ConsecutiveLossPause {
        streak: u32,
        windows_paused: u32,
    },
    GhostFillDetected {
        count_this_hour: u32,
    },
    FillRateTooLow {
        rate: f64,
        threshold: f64,
    },
    DailySummary {
        pnl: f64,
        fills: u32,
        attempts: u32,
        win_rate: f64,
        fill_rate: f64,
        best_regime: String,
    },
    BotStarted {
        balance_usd: f64,
    },
    BotStopped {
        final_pnl: f64,
    },
}

impl TelegramAlerter {
    pub fn new(config: &TelegramConfig) -> Self {
        let enabled = !config.bot_token.is_empty() && !config.chat_id.is_empty();
        if !enabled {
            info!("Telegram alerts disabled (bot_token or chat_id not set)");
        }

        Self {
            client: Client::new(),
            token: config.bot_token.clone(),
            chat_id: config.chat_id.clone(),
            enabled,
        }
    }

    /// Send an alert asynchronously. Failures are logged and never propagated.
    pub fn alert(&self, event: AlertEvent) {
        if !self.enabled {
            return;
        }

        let token = self.token.clone();
        let chat_id = self.chat_id.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            let text = format_event(&event);
            if let Err(err) = send_message(&client, &token, &chat_id, &text).await {
                error!("Telegram send failed: {}", err);
            }
        });
    }
}

fn format_event(event: &AlertEvent) -> String {
    match event {
        AlertEvent::DailyLossCap { loss } => {
            format!(
                "DAILY LOSS CAP HIT\nLoss: ${:.2}\nBot paused until midnight UTC.",
                loss
            )
        }
        AlertEvent::ConsecutiveLossPause {
            streak,
            windows_paused,
        } => {
            format!(
                "LOSS STREAK PAUSE\n{} consecutive losses\nSkipping next {} windows",
                streak, windows_paused
            )
        }
        AlertEvent::GhostFillDetected { count_this_hour } => {
            format!(
                "GHOST FILL DETECTED\n{} ghost fills this hour\nBot paused; check positions",
                count_this_hour
            )
        }
        AlertEvent::FillRateTooLow { rate, threshold } => {
            format!(
                "FILL RATE TOO LOW\nCurrent: {:.1}%\nThreshold: {:.1}%\nPossible liquidity problem",
                rate * 100.0,
                threshold * 100.0
            )
        }
        AlertEvent::DailySummary {
            pnl,
            fills,
            attempts,
            win_rate,
            fill_rate,
            best_regime,
        } => {
            format!(
                "DAILY SUMMARY\nPnL: ${:.2}\nFills: {}/{} ({:.1}% fill rate)\nWin rate: {:.1}%\nBest regime: {}",
                pnl,
                fills,
                attempts,
                fill_rate * 100.0,
                win_rate * 100.0,
                best_regime
            )
        }
        AlertEvent::BotStarted { balance_usd } => {
            format!("BOT STARTED\nBalance: ${:.2} USDC", balance_usd)
        }
        AlertEvent::BotStopped { final_pnl } => {
            format!("BOT STOPPED\nSession PnL: ${:.2}", final_pnl)
        }
    }
}

async fn send_message(client: &Client, token: &str, chat_id: &str, text: &str) -> Result<()> {
    let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
    let body = json!({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    });

    let resp = client
        .post(&url)
        .json(&body)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Telegram API error {}: {}", status, body);
    }

    Ok(())
}

/// Convenience no-op alerter for disabled alerting paths.
pub struct NoOpAlerter;

impl NoOpAlerter {
    pub fn alert(&self, _event: AlertEvent) {}
}
