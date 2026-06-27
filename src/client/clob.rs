use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedOrder {
    pub salt: String,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    pub token_id: String,
    pub maker_amount: String,
    pub taker_amount: String,
    pub expiration: String,
    pub nonce: String,
    pub fee_rate_bps: String,
    pub side: u8,
    pub signature_type: u8,
    pub signature: String,
}

#[derive(Debug, Clone)]
pub struct ClobClient {
    http_client: Client,
    order_url: String,
}

impl ClobClient {
    pub fn new(order_url: impl Into<String>) -> Self {
        Self {
            http_client: Client::new(),
            order_url: order_url.into(),
        }
    }

    pub fn http_client(&self) -> &Client {
        &self.http_client
    }

    pub fn order_url(&self) -> &str {
        &self.order_url
    }

    pub async fn get_open_position_token_ids(&self) -> Result<Vec<String>> {
        Err(anyhow::anyhow!(
            "get_open_position_token_ids() is not implemented yet"
        ))
    }
}
