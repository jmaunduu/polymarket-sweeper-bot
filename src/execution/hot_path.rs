//! THE HOT PATH. Stays minimal by law.

use crate::client::clob::ClobClient;
use crate::error::BotResult;
use crate::execution::prebuild::PrebuiltOrder;
use chrono::Utc;
use reqwest::header::{HeaderMap, HeaderValue};
use std::time::Instant;

#[inline(always)]
pub async fn fire(
    client: &ClobClient,
    prebuilt: &PrebuiltOrder,
    api_key: &str,
    passphrase: &str,
) -> BotResult<String> {
    let t0 = Instant::now();
    let ts = Utc::now().timestamp().to_string();

    let sig = prebuilt
        .hmac_key
        .sign(&ts, "POST", "/order", &prebuilt.body_bytes);

    let mut headers = HeaderMap::with_capacity(5);
    headers.insert("POLY-API-KEY", HeaderValue::from_str(api_key).unwrap());
    headers.insert("POLY-TIMESTAMP", HeaderValue::from_str(&ts).unwrap());
    headers.insert("POLY-SIGNATURE", HeaderValue::from_str(&sig).unwrap());
    headers.insert("POLY-PASSPHRASE", HeaderValue::from_str(passphrase).unwrap());
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );

    let resp = client
        .http_client()
        .post(client.order_url())
        .headers(headers)
        .body(prebuilt.body_bytes.clone())
        .send()
        .await?
        .text()
        .await?;

    let elapsed = t0.elapsed().as_micros();
    tracing::trace!("fire() completed in {}µs", elapsed);

    Ok(resp)
}
