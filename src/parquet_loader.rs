use std::{
    cmp::Ordering,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use arrow::datatypes::DataType;
use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int64Array, LargeListArray, LargeStringArray,
    ListArray, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, UInt64Array,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[derive(Debug, Clone)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone)]
pub struct FeedEvent {
    pub market_slug: String,
    pub timestamp: f64,
    pub local_timestamp: Option<f64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub bid_levels: Vec<Level>,
    pub ask_levels: Vec<Level>,
    pub pc_price: Option<f64>,
    pub pc_size: Option<f64>,
    pub pc_side: Option<String>,
    pub trade_price: Option<f64>,
    pub trade_size: Option<f64>,
    pub trade_side: Option<String>,
    pub winning_outcome: Option<String>,
    pub market_close_ts: f64,
    pub source_file: PathBuf,
}

impl FeedEvent {
    pub fn observed_price(&self) -> Option<f64> {
        self.pc_price
            .or(self.trade_price)
            .or(self.best_ask)
            .or(self.best_bid)
    }
}

pub fn load_parquet_events(parquet_dir: &Path) -> Result<Vec<FeedEvent>> {
    let files = list_parquet_files(parquet_dir)?;
    let mut events = Vec::new();
    for path in files {
        events.extend(load_parquet_file_events(&path)?);
    }

    events.sort_by(|left, right| {
        left.timestamp
            .partial_cmp(&right.timestamp)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.market_slug.cmp(&right.market_slug))
    });

    Ok(events)
}

pub fn list_parquet_files(parquet_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(parquet_dir)
        .with_context(|| format!("failed to read parquet directory {}", parquet_dir.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("parquet"))
        .collect();
    files.sort();

    if files.is_empty() {
        anyhow::bail!("no parquet files found in {}", parquet_dir.display());
    }

    Ok(files)
}

pub fn load_parquet_file_events(path: &Path) -> Result<Vec<FeedEvent>> {
    let (default_slug, market_close_ts) = parse_market_metadata(path)?;
    let file = File::open(path)
        .with_context(|| format!("failed to open parquet file {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("failed to create parquet reader for {}", path.display()))?;
    let mut reader = builder.with_batch_size(2048).build()?;
    let mut events = Vec::new();

    while let Some(batch) = reader.next() {
        let batch = batch?;
        decode_batch(&batch, path, &default_slug, market_close_ts, &mut events)?;
    }

    events.sort_by(|left, right| {
        left.timestamp
            .partial_cmp(&right.timestamp)
            .unwrap_or(Ordering::Equal)
    });

    Ok(events)
}

fn decode_batch(
    batch: &RecordBatch,
    source_file: &Path,
    default_slug: &str,
    market_close_ts: f64,
    out: &mut Vec<FeedEvent>,
) -> Result<()> {
    let schema = batch.schema();

    for row in 0..batch.num_rows() {
        let timestamp = column_index(schema.as_ref(), "timestamp")
            .and_then(|idx| get_timestamp_seconds(batch.column(idx).as_ref(), row))
            .or_else(|| {
                column_index(schema.as_ref(), "local_timestamp")
                    .and_then(|idx| get_timestamp_seconds(batch.column(idx).as_ref(), row))
            });

        let Some(timestamp) = timestamp else {
            continue;
        };

        let market_slug = column_index(schema.as_ref(), "market_slug")
            .and_then(|idx| get_string(batch.column(idx).as_ref(), row))
            .unwrap_or_else(|| default_slug.to_string());

        let ask_prices = column_index(schema.as_ref(), "ask_prices")
            .map(|idx| get_list_f64(batch.column(idx).as_ref(), row))
            .transpose()?
            .unwrap_or_default();
        let ask_sizes = column_index(schema.as_ref(), "ask_sizes")
            .map(|idx| get_list_f64(batch.column(idx).as_ref(), row))
            .transpose()?
            .unwrap_or_default();
        let bid_prices = column_index(schema.as_ref(), "bid_prices")
            .map(|idx| get_list_f64(batch.column(idx).as_ref(), row))
            .transpose()?
            .unwrap_or_default();
        let bid_sizes = column_index(schema.as_ref(), "bid_sizes")
            .map(|idx| get_list_f64(batch.column(idx).as_ref(), row))
            .transpose()?
            .unwrap_or_default();

        let ask_levels = zip_levels(&ask_prices, &ask_sizes);
        let bid_levels = zip_levels(&bid_prices, &bid_sizes);

        let best_ask = column_index(schema.as_ref(), "best_ask")
            .and_then(|idx| get_number(batch.column(idx).as_ref(), row))
            .or_else(|| ask_levels.first().map(|level| level.price));
        let best_bid = column_index(schema.as_ref(), "best_bid")
            .and_then(|idx| get_number(batch.column(idx).as_ref(), row))
            .or_else(|| bid_levels.first().map(|level| level.price));

        let event = FeedEvent {
            market_slug,
            timestamp,
            local_timestamp: column_index(schema.as_ref(), "local_timestamp")
                .and_then(|idx| get_timestamp_seconds(batch.column(idx).as_ref(), row)),
            best_bid,
            best_ask,
            bid_levels,
            ask_levels,
            pc_price: column_index(schema.as_ref(), "pc_price")
                .and_then(|idx| get_number(batch.column(idx).as_ref(), row)),
            pc_size: column_index(schema.as_ref(), "pc_size")
                .and_then(|idx| get_number(batch.column(idx).as_ref(), row)),
            pc_side: column_index(schema.as_ref(), "pc_side")
                .and_then(|idx| get_string(batch.column(idx).as_ref(), row)),
            trade_price: column_index(schema.as_ref(), "trade_price")
                .and_then(|idx| get_number(batch.column(idx).as_ref(), row)),
            trade_size: column_index(schema.as_ref(), "trade_size")
                .and_then(|idx| get_number(batch.column(idx).as_ref(), row)),
            trade_side: column_index(schema.as_ref(), "trade_side")
                .and_then(|idx| get_string(batch.column(idx).as_ref(), row)),
            winning_outcome: column_index(schema.as_ref(), "winning_outcome")
                .and_then(|idx| get_string(batch.column(idx).as_ref(), row)),
            market_close_ts,
            source_file: source_file.to_path_buf(),
        };

        if event.observed_price().is_some()
            || !event.ask_levels.is_empty()
            || !event.bid_levels.is_empty()
        {
            out.push(event);
        }
    }

    Ok(())
}

fn parse_market_metadata(path: &Path) -> Result<(String, f64)> {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| anyhow!("invalid parquet filename {}", path.display()))?;
    let parts: Vec<&str> = stem.split('-').collect();
    if parts.len() < 4 {
        anyhow::bail!("unexpected parquet filename format: {stem}");
    }
    let start_ts: f64 = parts
        .last()
        .ok_or_else(|| anyhow!("missing start timestamp in {stem}"))?
        .parse::<i64>()
        .with_context(|| format!("invalid start timestamp in {stem}"))?
        as f64;
    let duration_secs = if stem.contains("-5m-") {
        300.0
    } else if stem.contains("-15m-") {
        900.0
    } else {
        anyhow::bail!("unable to infer timeframe from parquet filename: {stem}");
    };

    Ok((stem.to_string(), start_ts + duration_secs))
}

fn zip_levels(prices: &[f64], sizes: &[f64]) -> Vec<Level> {
    prices
        .iter()
        .copied()
        .zip(sizes.iter().copied())
        .filter(|(price, size)| *price > 0.0 && *size > 0.0)
        .map(|(price, size)| Level { price, size })
        .collect()
}

fn column_index(schema: &arrow::datatypes::Schema, name: &str) -> Option<usize> {
    schema.fields().iter().position(|field| field.name() == name)
}

fn get_string(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Some(array.value(row).to_string());
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Some(array.value(row).to_string());
    }
    None
}

fn get_number(array: &dyn Array, row: usize) -> Option<f64> {
    if array.is_null(row) {
        return None;
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        return Some(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
        return Some(array.value(row) as f64);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(array.value(row) as f64);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        return Some(array.value(row) as f64);
    }
    None
}

fn get_timestamp_seconds(array: &dyn Array, row: usize) -> Option<f64> {
    if array.is_null(row) {
        return None;
    }
    if let Some(array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Some(array.value(row) as f64 / 1_000_000_000.0);
    }
    if let Some(array) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Some(array.value(row) as f64 / 1_000_000.0);
    }
    if let Some(array) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Some(array.value(row) as f64 / 1_000.0);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        let raw = array.value(row);
        return Some(if raw > 10_000_000_000_000 {
            raw as f64 / 1_000_000.0
        } else if raw > 10_000_000_000 {
            raw as f64 / 1_000.0
        } else {
            raw as f64
        });
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        let raw = array.value(row);
        return Some(if raw > 10_000_000_000_000 {
            raw as f64 / 1_000_000.0
        } else if raw > 10_000_000_000 {
            raw as f64 / 1_000.0
        } else {
            raw as f64
        });
    }
    None
}

fn get_list_f64(array: &dyn Array, row: usize) -> Result<Vec<f64>> {
    if array.is_null(row) {
        return Ok(Vec::new());
    }
    if let Some(array) = array.as_any().downcast_ref::<ListArray>() {
        return values_to_f64(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeListArray>() {
        return values_to_f64(array.value(row));
    }
    anyhow::bail!("unsupported parquet list type: {:?}", array.data_type());
}

fn values_to_f64(values: Arc<dyn Array>) -> Result<Vec<f64>> {
    match values.data_type() {
        DataType::Float64 => {
            let array = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("failed to downcast Float64Array"))?;
            Ok((0..array.len())
                .filter_map(|idx| (!array.is_null(idx)).then(|| array.value(idx)))
                .collect())
        }
        DataType::Float32 => {
            let array = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow!("failed to downcast Float32Array"))?;
            Ok((0..array.len())
                .filter_map(|idx| (!array.is_null(idx)).then(|| array.value(idx) as f64))
                .collect())
        }
        DataType::Int64 => {
            let array = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("failed to downcast Int64Array"))?;
            Ok((0..array.len())
                .filter_map(|idx| (!array.is_null(idx)).then(|| array.value(idx) as f64))
                .collect())
        }
        other => anyhow::bail!("unsupported parquet list value type: {other:?}"),
    }
}
