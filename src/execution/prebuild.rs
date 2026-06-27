//! Pre-built order structs and cached HMAC keys.
//!
//! Implementation:
//!   60 seconds before the trigger window, prebuild() is called.
//!   It computes:
//!     1. The signed EIP-712 order struct (all fields fixed)
//!     2. The serialised JSON body (no serialisation on hot path)
//!     3. The HMAC-SHA256 key object (only needs timestamp update at fire)

use crate::client::auth::{ApiCredentials, CachedHmacKey};
use crate::client::clob::SignedOrder;
use crate::config::HotPathConfig;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

#[derive(Clone)]
pub struct PrebuiltOrder {
    pub body_bytes: Vec<u8>,
    pub order: SignedOrder,
    pub hmac_key: CachedHmacKey,
    pub token_id: String,
    pub built_at: Instant,
    pub price: f64,
    pub size_usd: f64,
}

pub struct PrebuildCache {
    inner: Arc<DashMap<String, PrebuiltOrder>>,
    config: HotPathConfig,
}

impl PrebuildCache {
    pub fn new(config: HotPathConfig) -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            config,
        }
    }

    pub fn insert(&self, order: PrebuiltOrder) {
        debug!(
            "Prebuilt order cached for {}",
            &order.token_id[..20.min(order.token_id.len())]
        );
        self.inner.insert(order.token_id.clone(), order);
    }

    pub fn get(&self, token_id: &str) -> Option<PrebuiltOrder> {
        self.inner.get(token_id).and_then(|p| {
            if p.built_at.elapsed().as_secs() > self.config.prebuild_max_age_secs {
                None
            } else {
                Some(p.clone())
            }
        })
    }

    pub fn remove(&self, token_id: &str) {
        self.inner.remove(token_id);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

pub async fn build_order(
    token_id: &str,
    price: f64,
    size_usd: f64,
    creds: &ApiCredentials,
    maker_addr: &str,
    signer: &crate::client::auth::OrderSigner,
) -> anyhow::Result<PrebuiltOrder> {
    let salt = uuid::Uuid::new_v4().to_string();
    let contracts = size_usd / price;
    let maker_amount = (contracts * 1_000_000.0) as u64;
    let taker_amount = (size_usd * 1_000_000.0) as u64;

    let order = SignedOrder {
        salt: salt.clone(),
        maker: maker_addr.to_string(),
        signer: maker_addr.to_string(),
        taker: "0x0000000000000000000000000000000000000000".to_string(),
        token_id: token_id.to_string(),
        maker_amount: maker_amount.to_string(),
        taker_amount: taker_amount.to_string(),
        expiration: "0".to_string(),
        nonce: "0".to_string(),
        fee_rate_bps: "0".to_string(),
        side: 1,
        signature_type: 0,
        signature: signer.sign_order_struct(maker_addr, token_id, maker_amount, taker_amount)?,
    };

    let body_bytes = serde_json::to_vec(&serde_json::json!({
        "order": order,
        "orderType": "GTC"
    }))?;

    let hmac_key = CachedHmacKey::new(creds.api_secret.as_bytes())?;

    Ok(PrebuiltOrder {
        body_bytes,
        order,
        hmac_key,
        token_id: token_id.to_string(),
        built_at: Instant::now(),
        price,
        size_usd,
    })
}
