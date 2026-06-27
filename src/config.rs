use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub api: ApiConfig,
    pub feed: FeedConfig,
    pub strategy: StrategyConfig,
    pub data: DataConfig,
    pub ntp: NtpConfig,
    pub cpu: CpuConfig,
    pub hot_path: HotPathConfig,
    pub regime: RegimeConfig,
    pub risk: RiskConfig,
    pub dry_run: DryRunConfig,
    pub telegram: TelegramConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    pub clob_host: String,
    pub gamma_api: String,
    pub ws_url: String,
    pub chain_id: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeedConfig {
    pub parallel_connections: usize,
    pub stagger_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    pub sweep_trigger_prob: f64,
    pub time_trigger_secs: f64,
    pub assets: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataConfig {
    pub duckdb_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NtpConfig {
    pub server: String,
    pub max_drift_ms: f64,
    pub check_interval_mins: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CpuConfig {
    pub websocket_core: i32,
    pub signal_core: i32,
    pub order_core: i32,
    pub warn_if_disabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HotPathConfig {
    pub prebuild_lead_secs: f64,
    pub prebuild_max_age_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegimeConfig {
    pub weekday_enabled: bool,
    pub weekend_enabled: bool,
    pub allowed_utc_hours: Vec<u32>,
    pub log_regime_stats_mins: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub max_daily_loss_usd: f64,
    pub consecutive_loss_pause: u32,
    pub pause_windows: u32,
    pub min_fill_rate: f64,
    pub max_ghost_fills_per_hour: u32,
    pub max_session_drawdown_pct: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DryRunConfig {
    pub enabled: bool,
    pub tolerance_pct: f64,
    pub min_sample_windows: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramConfig {
    pub bot_token: String,
    pub chat_id: String,
    pub alert_on_daily_cap: bool,
    pub alert_on_loss_pause: bool,
    pub alert_on_ghost_fill: bool,
    pub alert_on_daily_summary: bool,
    pub daily_summary_utc_hour: u32,
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file at {path}"))?;
        let cfg = toml::from_str(&raw).context("failed to parse config.toml")?;
        Ok(cfg)
    }
}
