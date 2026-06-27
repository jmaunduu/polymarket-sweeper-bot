mod client;
mod config;
mod data;
mod error;
mod execution;
mod feed;
mod monitor;
mod risk;
mod strategy;
mod sweeper;

use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::{Timelike, Utc};
use client::clob::ClobClient;
use config::Config;
use data::Recorder;
use dotenv::dotenv;
use feed::{spawn_websocket_feed, MarketEvent};
use monitor::alerts::{AlertEvent, TelegramAlerter};
use risk::controls::{RiskControls, RiskStatus};
use strategy::regime::{RegimeFilter, RegimeKey, RegimeTracker};
use sweeper::{Market, Opportunity, Sweeper, TokenContext};
use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let mut config = Config::load("config.toml")?;
    apply_env_overrides(&mut config);
    std::fs::create_dir_all("data").context("failed to create data directory")?;

    monitor::ntp::enforce_sync(&config.ntp.server, config.ntp.max_drift_ms).await?;
    monitor::ntp::spawn_periodic_check(
        config.ntp.server.clone(),
        config.ntp.max_drift_ms,
        config.ntp.check_interval_mins,
    );

    let dry_run = env_flag("DRY_RUN") || config.dry_run.enabled;
    if dry_run {
        info!("DRY RUN MODE ACTIVE - orders will not be submitted");
    }

    let alerter = Arc::new(TelegramAlerter::new(&config.telegram));
    let usdc = 0.0;
    alerter.alert(AlertEvent::BotStarted { balance_usd: usdc });

    let duckdb_path = env::var("DUCKDB_PATH").unwrap_or_else(|_| config.data.duckdb_path.clone());
    let recorder = Recorder::new(&duckdb_path)
        .map_err(|err| anyhow::anyhow!("failed to initialize DuckDB recorder: {err}"))?;

    info!(
        clob_host = %config.api.clob_host,
        chain_id = config.api.chain_id,
        trigger_prob = config.strategy.sweep_trigger_prob,
        time_trigger_secs = config.strategy.time_trigger_secs,
        "starting polymarket sweeper bot"
    );

    let required_env = ["PK", "FUNDER_ADDRESS", "POLYGON_RPC"];
    for key in required_env {
        if env::var(key).unwrap_or_default().is_empty() {
            warn!("{key} is not set in .env; dry-run market monitoring still works, but live trading setup is incomplete");
        }
    }

    let sweeper = Sweeper::new(
        config.strategy.sweep_trigger_prob,
        config.strategy.time_trigger_secs,
    );

    let markets = sweeper
        .discover_markets(&config.api.gamma_api, &config.strategy.assets)
        .await
        .context("failed to discover configured crypto markets from Gamma")?;
    let (mut token_contexts, asset_ids) = build_token_contexts(&markets);
    let mut watched_asset_ids: HashSet<String> = asset_ids.iter().cloned().collect();
    log_market_watch(
        "subscribing websocket feed",
        &markets,
        asset_ids.len(),
        &config,
    );

    let (tx, mut rx) = mpsc::unbounded_channel::<MarketEvent>();
    let mut feed_handle = tokio::spawn(spawn_websocket_feed(
        asset_ids,
        config.api.ws_url.clone(),
        config.feed.parallel_connections,
        config.feed.stagger_ms,
        tx.clone(),
    ));

    let clob_client = Arc::new(ClobClient::new(format!(
        "{}/order",
        config.api.clob_host.trim_end_matches('/')
    )));
    let (risk_controls, _hot_metrics) = RiskControls::new(config.risk.clone(), alerter.clone());
    let risk = Arc::new(risk_controls);
    let ghost_detector = Arc::new(data::ghost_fill_detector::GhostFillDetector::new());
    ghost_detector
        .clone()
        .spawn_checker(clob_client.clone(), risk.clone(), 30);

    let regime_filter = RegimeFilter::new(config.regime.clone());
    let regime_tracker = Arc::new(RegimeTracker::new());
    if config.regime.log_regime_stats_mins > 0 {
        let rt = regime_tracker.clone();
        let interval_mins = config.regime.log_regime_stats_mins;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(interval_mins * 60));
            loop {
                interval.tick().await;
                rt.log_summary();
            }
        });
    }

    if config.telegram.daily_summary_utc_hour < 24 {
        let alerter_c = alerter.clone();
        let risk_c = risk.clone();
        let tracker_c = regime_tracker.clone();
        let summary_hour = config.telegram.daily_summary_utc_hour;
        let summary_enabled = config.telegram.alert_on_daily_summary;
        tokio::spawn(async move {
            let mut last_summary_day = None;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                let now = Utc::now();
                let today = now.date_naive();
                if now.hour() != summary_hour || last_summary_day == Some(today) {
                    continue;
                }

                let snap = risk_c.snapshot();
                if summary_enabled {
                    alerter_c.alert(AlertEvent::DailySummary {
                        pnl: snap.daily_pnl,
                        fills: snap.fill_successes,
                        attempts: snap.fill_attempts,
                        win_rate: snap.recent_win_rate,
                        fill_rate: snap.fill_rate,
                        best_regime: tracker_c.best_regime_label(),
                    });
                }
                risk_c.daily_reset();
                last_summary_day = Some(today);
            }
        });
    } else {
        warn!(
            daily_summary_utc_hour = config.telegram.daily_summary_utc_hour,
            "daily summary hour is outside 0-23; scheduler disabled"
        );
    }

    let mut triggered_tokens = HashSet::new();
    let mut market_prices = HashMap::new();
    let mut market_refresh = tokio::time::interval(Duration::from_secs(60));
    market_refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
    market_refresh.tick().await;

    loop {
        tokio::select! {
            _ = market_refresh.tick() => {
                match sweeper
                    .discover_markets(&config.api.gamma_api, &config.strategy.assets)
                    .await
                {
                    Ok(markets) => {
                        let (fresh_contexts, fresh_asset_ids) = build_token_contexts(&markets);
                        let fresh_asset_set: HashSet<String> =
                            fresh_asset_ids.iter().cloned().collect();

                        token_contexts = fresh_contexts;
                        triggered_tokens.retain(|token_id| fresh_asset_set.contains(token_id));
                        market_prices.retain(|token_id, _| fresh_asset_set.contains(token_id));

                        if fresh_asset_set != watched_asset_ids {
                            feed_handle.abort();
                            log_market_watch(
                                "refreshing websocket feed with rediscovered markets",
                                &markets,
                                fresh_asset_ids.len(),
                                &config,
                            );
                            feed_handle = tokio::spawn(spawn_websocket_feed(
                                fresh_asset_ids,
                                config.api.ws_url.clone(),
                                config.feed.parallel_connections,
                                config.feed.stagger_ms,
                                tx.clone(),
                            ));
                        } else {
                            info!(
                                markets = markets.len(),
                                asset_ids = fresh_asset_set.len(),
                                "refreshed market metadata"
                            );
                        }

                        watched_asset_ids = fresh_asset_set;
                    }
                    Err(err) => {
                        warn!(error = %err, "failed to refresh crypto markets from Gamma");
                    }
                }
            }
            maybe_event = rx.recv() => {
                let Some(event) = maybe_event else {
                    warn!("websocket event channel closed");
                    break;
                };

                let tick = event.tick();
                market_prices.insert(tick.asset_id.clone(), tick.price);
                let context = token_contexts.get(&tick.asset_id).cloned();
                let market_id = context
                    .as_ref()
                    .map(|ctx| ctx.market_id.clone())
                    .unwrap_or_else(|| tick.market_id.clone());
                let outcome = context.as_ref().map(|ctx| ctx.outcome.clone());

                if let Err(err) = recorder.record_tick(
                    tick.received_at.timestamp_millis() as f64 / 1_000.0,
                    &tick.asset_id,
                    tick.price,
                    outcome.as_deref(),
                    Some(&market_id),
                    tick.event_type.as_str(),
                    tick.side.as_deref(),
                    tick.size,
                    tick.event_timestamp.as_deref(),
                    Some(tick.connection_id as i64),
                    &tick.raw_json,
                ).await {
                    error!(error = %err, "failed to record tick");
                }

                if !regime_filter.is_allowed() {
                    continue;
                }

                match risk.check() {
                    RiskStatus::Clear => {}
                    status => {
                        debug!(?status, "risk gate blocked opportunity evaluation");
                        continue;
                    }
                }

                if let Some(ctx) = context.as_ref() {
                    let now = Utc::now();
                    let secs_remaining = (ctx.end_date - now).num_milliseconds() as f64 / 1_000.0;

                    if let Some(opportunity) = sweeper
                        .evaluate(ctx, tick.price, now)
                        .map(|opportunity| opportunity.with_token_id(&tick.asset_id))
                    {
                        if triggered_tokens.insert(opportunity.token_id.clone()) {
                            let regime_key = RegimeKey::current();
                            let regime_label = regime_key.label();
                            let mut filled = false;
                            let win = false;
                            let pnl = 0.0;

                            print_opportunity(&opportunity);
                            ghost_detector.register_order(opportunity.token_id.clone());

                            if dry_run {
                                info!(
                                    "DRY RUN: WOULD BID {} @ {} x {}",
                                    &opportunity.token_id[..12.min(opportunity.token_id.len())],
                                    opportunity.price,
                                    0.0_f64,
                                );
                                if let Err(err) = recorder.record_bot_trade(
                                    tick.received_at.timestamp_millis() as f64 / 1_000.0,
                                    &opportunity.market_id,
                                    &opportunity.token_id,
                                    opportunity.price,
                                    0.0,
                                    false,
                                    Some(config.strategy.sweep_trigger_prob),
                                    Some(secs_remaining),
                                    Some(regime_label.as_str()),
                                    true,
                                    None,
                                ) {
                                    error!(
                                        error = %err,
                                        token_id = %opportunity.token_id,
                                        "failed to record dry run opportunity"
                                    );
                                }
                            } else {
                                let prebuild_cache = crate::execution::prebuild::PrebuildCache::new(
                                    config.hot_path.clone(),
                                );
                                match prebuild_cache.get(&opportunity.token_id) {
                                    Some(prebuilt) => {
                                        let t0 = std::time::Instant::now();
                                        match crate::execution::hot_path::fire(
                                            clob_client.as_ref(),
                                            &prebuilt,
                                            &env::var("POLY_API_KEY").unwrap_or_default(),
                                            &env::var("POLY_API_PASSPHRASE").unwrap_or_default(),
                                        )
                                        .await
                                        {
                                            Ok(resp) => {
                                                let latency_ms =
                                                    t0.elapsed().as_secs_f64() * 1000.0;
                                                filled = true;
                                                info!(
                                                    "Order placed in {:.1}ms: {}",
                                                    latency_ms,
                                                    &resp[..50.min(resp.len())]
                                                );
                                                if let Err(err) = recorder.record_bot_trade(
                                                    tick.received_at.timestamp_millis() as f64
                                                        / 1_000.0,
                                                    &opportunity.market_id,
                                                    &opportunity.token_id,
                                                    opportunity.price,
                                                    prebuilt.size_usd,
                                                    filled,
                                                    Some(config.strategy.sweep_trigger_prob),
                                                    Some(secs_remaining),
                                                    Some(regime_label.as_str()),
                                                    false,
                                                    Some(latency_ms),
                                                ) {
                                                    error!(
                                                        error = %err,
                                                        token_id = %opportunity.token_id,
                                                        "failed to record live opportunity"
                                                    );
                                                }
                                            }
                                            Err(err) => {
                                                error!(error = %err, "fire failed");
                                                if let Err(db_err) = recorder.record_bot_trade(
                                                    tick.received_at.timestamp_millis() as f64
                                                        / 1_000.0,
                                                    &opportunity.market_id,
                                                    &opportunity.token_id,
                                                    opportunity.price,
                                                    prebuilt.size_usd,
                                                    false,
                                                    Some(config.strategy.sweep_trigger_prob),
                                                    Some(secs_remaining),
                                                    Some(regime_label.as_str()),
                                                    false,
                                                    None,
                                                ) {
                                                    error!(
                                                        error = %db_err,
                                                        token_id = %opportunity.token_id,
                                                        "failed to record failed live opportunity"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    None => {
                                        warn!(
                                            "No prebuilt order for {} - placing live (slower)",
                                            &opportunity.token_id[..12.min(opportunity.token_id.len())]
                                        );
                                        if let Err(err) = recorder.record_bot_trade(
                                            tick.received_at.timestamp_millis() as f64 / 1_000.0,
                                            &opportunity.market_id,
                                            &opportunity.token_id,
                                            opportunity.price,
                                            0.0,
                                            false,
                                            Some(config.strategy.sweep_trigger_prob),
                                            Some(secs_remaining),
                                            Some(regime_label.as_str()),
                                            false,
                                            None,
                                        ) {
                                            error!(
                                                error = %err,
                                                token_id = %opportunity.token_id,
                                                "failed to record un-prebuilt live opportunity"
                                            );
                                        }
                                    }
                                }
                            }

                            regime_tracker.record(regime_key, filled, win, pnl);
                            risk.record_window(filled, win, pnl);
                        }
                    }
                }
            }
            ctrl_c = signal::ctrl_c() => {
                if let Err(err) = ctrl_c {
                    error!(error = %err, "failed to listen for ctrl-c");
                }
                info!("shutdown signal received");
                break;
            }
        }
    }

    feed_handle.abort();
    alerter.alert(AlertEvent::BotStopped {
        final_pnl: risk.snapshot().daily_pnl,
    });
    Ok(())
}

fn build_token_contexts(markets: &[Market]) -> (HashMap<String, TokenContext>, Vec<String>) {
    let mut token_contexts = HashMap::new();
    let mut asset_ids = Vec::new();

    for market in markets {
        let token = &market.yes;
        asset_ids.push(token.token_id.clone());
        token_contexts.insert(
            token.token_id.clone(),
            TokenContext {
                market_id: market.market_id.clone(),
                question: market.question.clone(),
                end_date: market.end_date,
                outcome: token.outcome.clone(),
            },
        );
    }

    asset_ids.sort();
    asset_ids.dedup();
    (token_contexts, asset_ids)
}

fn log_market_watch(message: &str, markets: &[Market], asset_ids: usize, config: &Config) {
    info!(
        markets = markets.len(),
        asset_ids,
        configured_assets = ?config.strategy.assets,
        parallel_connections = config.feed.parallel_connections,
        "{message}"
    );
}

fn print_opportunity(opportunity: &Opportunity) {
    println!(
        "DRY RUN opportunity | action=SELL YES | market={} | outcome={} | price={:.4} | seconds_remaining={:.2} | token_id={} | question={}",
        opportunity.market_id,
        opportunity.outcome,
        opportunity.price,
        opportunity.seconds_remaining,
        opportunity.token_id,
        opportunity.question
    );
}

fn apply_env_overrides(config: &mut Config) {
    if let Ok(bot_token) = env::var("TELEGRAM_BOT_TOKEN") {
        if !bot_token.is_empty() {
            config.telegram.bot_token = bot_token;
        }
    }

    if let Ok(chat_id) = env::var("TELEGRAM_CHAT_ID") {
        if !chat_id.is_empty() {
            config.telegram.chat_id = chat_id;
        }
    }
}

fn env_flag(key: &str) -> bool {
    env::var(key)
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}
