use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct Tick {
    pub connection_id: usize,
    pub event_type: EventType,
    pub market_id: String,
    pub asset_id: String,
    pub price: f64,
    pub side: Option<String>,
    pub size: Option<f64>,
    pub event_timestamp: Option<String>,
    pub received_at: DateTime<Utc>,
    pub raw_json: String,
}

#[derive(Debug, Clone, Copy)]
pub enum EventType {
    PriceChange,
    LastTradePrice,
}

impl EventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PriceChange => "price_change",
            Self::LastTradePrice => "last_trade_price",
        }
    }
}

#[derive(Debug, Clone)]
pub enum MarketEvent {
    PriceChange(Tick),
    LastTradePrice(Tick),
}

impl MarketEvent {
    pub fn tick(&self) -> &Tick {
        match self {
            Self::PriceChange(tick) | Self::LastTradePrice(tick) => tick,
        }
    }
}

#[derive(Debug, Serialize)]
struct SubscribeMessage {
    #[serde(rename = "assets_ids")]
    asset_ids: Vec<String>,
    #[serde(rename = "type")]
    message_type: String,
    custom_feature_enabled: bool,
}

pub async fn spawn_websocket_feed(
    asset_ids: Vec<String>,
    ws_url: String,
    parallel_connections: usize,
    stagger_ms: u64,
    tx: mpsc::UnboundedSender<MarketEvent>,
) -> anyhow::Result<()> {
    if asset_ids.is_empty() {
        return Ok(());
    }

    let mut handles = Vec::new();
    for connection_id in 0..parallel_connections.max(1) {
        let assets = asset_ids.clone();
        let sender = tx.clone();
        let url = ws_url.clone();
        let delay = Duration::from_millis(connection_id as u64 * stagger_ms);

        handles.push(tokio::spawn(async move {
            if !delay.is_zero() {
                sleep(delay).await;
            }
            run_connection(connection_id, assets, url, sender).await;
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}

async fn run_connection(
    connection_id: usize,
    asset_ids: Vec<String>,
    ws_url: String,
    tx: mpsc::UnboundedSender<MarketEvent>,
) {
    loop {
        match connect_async(&ws_url).await {
            Ok((stream, _)) => {
                let (mut write, mut read) = stream.split();
                let subscribe = SubscribeMessage {
                    asset_ids: asset_ids.clone(),
                    message_type: "market".to_string(),
                    custom_feature_enabled: true,
                };

                match serde_json::to_string(&subscribe) {
                    Ok(payload) => {
                        if let Err(err) = write.send(Message::Text(payload)).await {
                            warn!(connection_id, error = %err, "failed to subscribe websocket");
                            sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                    Err(err) => {
                        warn!(connection_id, error = %err, "failed to serialize websocket subscription");
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }

                while let Some(message) = read.next().await {
                    match message {
                        Ok(Message::Text(raw)) => {
                            if let Err(err) = dispatch_message(connection_id, &raw, &tx) {
                                debug!(connection_id, error = %err, "ignored websocket payload");
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if let Err(err) = write.send(Message::Pong(payload)).await {
                                warn!(connection_id, error = %err, "failed to respond to ping");
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => break,
                        Ok(_) => {}
                        Err(err) => {
                            warn!(connection_id, error = %err, "websocket read error");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                warn!(connection_id, error = %err, "websocket connect failed");
            }
        }

        sleep(Duration::from_secs(1)).await;
    }
}

fn dispatch_message(
    connection_id: usize,
    raw: &str,
    tx: &mpsc::UnboundedSender<MarketEvent>,
) -> anyhow::Result<()> {
    let payload: Value = serde_json::from_str(raw).context("failed to parse websocket json")?;
    let event_type = payload
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or_default();

    match event_type {
        "price_change" => {
            let market_id = payload
                .get("market")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let event_timestamp = payload
                .get("timestamp")
                .and_then(Value::as_str)
                .map(ToString::to_string);

            if let Some(changes) = payload.get("price_changes").and_then(Value::as_array) {
                for change in changes {
                    if let Some(tick) = build_tick(
                        connection_id,
                        EventType::PriceChange,
                        &market_id,
                        event_timestamp.clone(),
                        change,
                        raw,
                    ) {
                        let _ = tx.send(MarketEvent::PriceChange(tick));
                    }
                }
            }
        }
        "last_trade_price" => {
            if let Some(tick) = build_tick(
                connection_id,
                EventType::LastTradePrice,
                payload
                    .get("market")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                payload
                    .get("timestamp")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                &payload,
                raw,
            ) {
                let _ = tx.send(MarketEvent::LastTradePrice(tick));
            }
        }
        _ => {}
    }

    Ok(())
}

fn build_tick(
    connection_id: usize,
    event_type: EventType,
    market_id: &str,
    event_timestamp: Option<String>,
    payload: &Value,
    raw_json: &str,
) -> Option<Tick> {
    let asset_id = payload
        .get("asset_id")
        .and_then(Value::as_str)?
        .to_string();
    let price = payload.get("price").and_then(parse_f64)?;
    let side = payload
        .get("side")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let size = payload.get("size").and_then(parse_f64);

    Some(Tick {
        connection_id,
        event_type,
        market_id: market_id.to_string(),
        asset_id,
        price,
        side,
        size,
        event_timestamp,
        received_at: Utc::now(),
        raw_json: raw_json.to_string(),
    })
}

fn parse_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}
