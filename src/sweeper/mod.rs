use anyhow::{anyhow, Context};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Market {
    pub market_id: String,
    pub question: String,
    pub end_date: DateTime<Utc>,
    pub yes: OutcomeToken,
    pub no: OutcomeToken,
}

#[derive(Debug, Clone)]
pub struct OutcomeToken {
    pub token_id: String,
    pub outcome: String,
    pub price: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct TokenContext {
    pub market_id: String,
    pub question: String,
    pub end_date: DateTime<Utc>,
    pub outcome: String,
}

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub market_id: String,
    pub question: String,
    pub outcome: String,
    pub token_id: String,
    pub price: f64,
    pub seconds_remaining: f64,
}

pub struct Sweeper {
    trigger_prob: f64,
    time_trigger_secs: f64,
    client: Client,
}

impl Sweeper {
    pub fn new(trigger_prob: f64, time_trigger_secs: f64) -> Self {
        Self {
            trigger_prob,
            time_trigger_secs,
            client: Client::builder().tcp_nodelay(true).build().unwrap(),
        }
    }

    pub async fn discover_markets(
        &self,
        gamma_api: &str,
        assets: &[String],
    ) -> anyhow::Result<Vec<Market>> {
        const TIMEFRAMES: [&str; 1] = ["5m"];
        let normalized_assets: Vec<String> = if assets.is_empty() {
            vec!["btc".to_string()]
        } else {
            assets
                .iter()
                .map(|asset| asset.trim().to_ascii_lowercase())
                .filter(|asset| !asset.is_empty())
                .collect()
        };

        loop {
            let mut discovered_markets = Vec::new();

            for asset in &normalized_assets {
                for timeframe in TIMEFRAMES {
                    let slug = format!("{asset}-up-or-down-{timeframe}");
                    let response = self
                        .client
                        .get(format!("{}/markets", gamma_api.trim_end_matches('/')))
                        .query(&[
                            ("seriesSlug", slug.as_str()),
                            ("active", "true"),
                            ("closed", "false"),
                        ])
                        .send()
                        .await
                        .with_context(|| {
                            format!("failed to fetch Gamma markets for seriesSlug={slug}")
                        })?;

                    let markets = response
                        .error_for_status()
                        .with_context(|| {
                            format!("Gamma returned a non-success status for seriesSlug={slug}")
                        })?
                        .json::<Vec<Value>>()
                        .await
                        .with_context(|| {
                            format!("failed to decode Gamma response for seriesSlug={slug}")
                        })?;

                    for market in &markets {
                        if let Ok(parsed) = parse_market(market) {
                            discovered_markets.push(parsed);
                        }
                    }
                }
            }

            discovered_markets.sort_by(|left, right| left.market_id.cmp(&right.market_id));
            discovered_markets.dedup_by(|left, right| left.market_id == right.market_id);

            if !discovered_markets.is_empty() {
                return Ok(discovered_markets);
            }

            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    }

    pub fn evaluate(
        &self,
        context: &TokenContext,
        price: f64,
        now: DateTime<Utc>,
    ) -> Option<Opportunity> {
        if !context.outcome.eq_ignore_ascii_case("yes") {
            return None;
        }

        let seconds_remaining =
            (context.end_date - now).num_milliseconds() as f64 / 1_000.0;

        if price < self.trigger_prob {
            return None;
        }

        if !(0.0..self.time_trigger_secs).contains(&seconds_remaining) {
            return None;
        }

        Some(Opportunity {
            market_id: context.market_id.clone(),
            question: context.question.clone(),
            outcome: context.outcome.clone(),
            token_id: String::new(),
            price,
            seconds_remaining,
        })
    }
}

impl Opportunity {
    pub fn with_token_id(mut self, token_id: &str) -> Self {
        self.token_id = token_id.to_string();
        self
    }
}

fn parse_market(market: &Value) -> anyhow::Result<Market> {
    let market_id = string_field(market, &["conditionId", "condition_id", "id"])?;
    let question = string_field(market, &["question"])?;
    let end_date_raw = string_field(market, &["endDate", "end_date"])?;
    let end_date = DateTime::parse_from_rfc3339(&end_date_raw)
        .with_context(|| format!("invalid endDate for market {market_id}"))?
        .with_timezone(&Utc);

    let outcomes = parse_outcomes(market);
    let token_ids = parse_token_ids(market);
    let prices = parse_prices(market);

    let tokens = if let Some(tokens_array) = market.get("tokens").and_then(Value::as_array) {
        let mut tokens = Vec::new();
        for token in tokens_array {
            let token_id = token
                .get("token_id")
                .or_else(|| token.get("tokenId"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if token_id.is_empty() {
                continue;
            }

            let outcome = token
                .get("outcome")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .unwrap_or_else(|| infer_outcome(tokens.len()));
            let price = token.get("price").and_then(parse_f64);
            tokens.push(OutcomeToken {
                token_id,
                outcome,
                price,
            });
        }
        tokens
    } else {
        token_ids
            .into_iter()
            .enumerate()
            .map(|(idx, token_id)| OutcomeToken {
                token_id,
                outcome: outcomes
                    .get(idx)
                    .cloned()
                    .unwrap_or_else(|| infer_outcome(idx)),
                price: prices.get(idx).copied(),
            })
            .collect()
    };

    if tokens.len() < 2 {
        return Err(anyhow!("market {market_id} does not have enough tokens"));
    }

    let yes = tokens
        .iter()
        .find(|token| token.outcome.eq_ignore_ascii_case("yes"))
        .cloned()
        .unwrap_or_else(|| tokens[0].clone());
    let no = tokens
        .iter()
        .find(|token| token.outcome.eq_ignore_ascii_case("no"))
        .cloned()
        .unwrap_or_else(|| tokens[1].clone());

    Ok(Market {
        market_id,
        question,
        end_date,
        yes,
        no,
    })
}

fn string_field(value: &Value, keys: &[&str]) -> anyhow::Result<String> {
    for key in keys {
        if let Some(found) = value.get(key).and_then(Value::as_str) {
            return Ok(found.to_string());
        }
    }
    Err(anyhow!("missing required string field: {}", keys.join(", ")))
}

fn parse_outcomes(market: &Value) -> Vec<String> {
    if let Some(raw) = market.get("outcomes").and_then(Value::as_str) {
        if let Ok(values) = serde_json::from_str::<Vec<String>>(raw) {
            return values;
        }
    }

    market
        .get("tokens")
        .and_then(Value::as_array)
        .map(|tokens| {
            tokens
                .iter()
                .filter_map(|token| token.get("outcome").and_then(Value::as_str))
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

pub fn parse_token_ids(market: &Value) -> Vec<String> {
    if let Some(raw) = market.get("clobTokenIds").and_then(Value::as_str) {
        if let Ok(ids) = serde_json::from_str::<Vec<String>>(raw) {
            return ids;
        }
    }

    market
        .get("tokens")
        .and_then(Value::as_array)
        .map(|tokens| {
            tokens
                .iter()
                .filter_map(|token| {
                    token
                        .get("token_id")
                        .or_else(|| token.get("tokenId"))
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

pub fn parse_prices(market: &Value) -> Vec<f64> {
    if let Some(raw) = market.get("outcomePrices").and_then(Value::as_str) {
        if let Ok(prices) = serde_json::from_str::<Vec<f64>>(raw) {
            return prices;
        }
        if let Ok(price_strings) = serde_json::from_str::<Vec<String>>(raw) {
            return price_strings
                .iter()
                .filter_map(|value| value.parse::<f64>().ok())
                .collect();
        }
    }

    market
        .get("tokens")
        .and_then(Value::as_array)
        .map(|tokens| {
            tokens
                .iter()
                .filter_map(|token| token.get("price").and_then(parse_f64))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn infer_outcome(index: usize) -> String {
    match index {
        0 => "Yes".to_string(),
        1 => "No".to_string(),
        _ => format!("Outcome{index}"),
    }
}
