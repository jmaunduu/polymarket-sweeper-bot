use std::path::PathBuf;

use anyhow::Result;

use crate::parquet_loader::{load_parquet_events, FeedEvent};

pub enum DataSource {
    Live { ws_url: String },
    Backtest { parquet_dir: PathBuf },
}

impl DataSource {
    pub fn load_feed_events(&self) -> Result<Vec<FeedEvent>> {
        match self {
            Self::Backtest { parquet_dir } => load_parquet_events(parquet_dir),
            Self::Live { ws_url } => anyhow::bail!(
                "live data source is not used by the backtest binary: {ws_url}"
            ),
        }
    }
}
