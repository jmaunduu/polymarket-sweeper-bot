use anyhow::Result;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct ApiCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
}

#[derive(Debug, Clone)]
pub struct OrderSigner {
    private_key: String,
}

impl OrderSigner {
    pub fn new(private_key: impl Into<String>) -> Self {
        Self {
            private_key: private_key.into(),
        }
    }

    pub fn private_key(&self) -> &str {
        &self.private_key
    }

    /// Signs a Polymarket CLOB V2 order struct using EIP-712.
    ///
    /// EIP-712 domain for Polymarket CLOB V2 on Polygon mainnet (chain 137):
    ///   name:              "Polymarket CTF Exchange"
    ///   version:           "1"
    ///   chainId:           137
    ///   verifyingContract: 0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E
    ///
    /// Order type hash (keccak256 of type string):
    ///   Order(address maker,address taker,address tokenId,
    ///         uint256 makerAmount,uint256 takerAmount,
    ///         uint256 expiration,uint256 nonce,uint256 feeRateBps,
    ///         uint8 side,uint8 signatureType)
    ///
    /// IMPORTANT: verify the verifying contract address against the current
    /// CLOB V2 docs before deploying. It has changed before between versions.
    pub fn sign_order_struct(
        &self,
        maker: &str,
        token_id: &str,
        maker_amount: u64,
        taker_amount: u64,
    ) -> Result<String> {
        use alloy_primitives::{keccak256, Address, U256};
        use std::str::FromStr;

        let domain_type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        let name_hash = keccak256(b"Polymarket CTF Exchange");
        let version_hash = keccak256(b"1");
        let chain_id = U256::from(137u64);
        let contract = Address::from_str("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
            .map_err(|e| anyhow::anyhow!("Bad contract address: {}", e))?;

        let mut domain_buf = [0u8; 160];
        domain_buf[0..32].copy_from_slice(domain_type_hash.as_slice());
        domain_buf[32..64].copy_from_slice(name_hash.as_slice());
        domain_buf[64..96].copy_from_slice(version_hash.as_slice());
        chain_id
            .to_be_bytes::<32>()
            .iter()
            .enumerate()
            .for_each(|(i, b)| domain_buf[96 + i] = *b);
        let mut addr_padded = [0u8; 32];
        addr_padded[12..].copy_from_slice(contract.as_slice());
        domain_buf[128..160].copy_from_slice(&addr_padded);

        let domain_separator = keccak256(&domain_buf);

        let order_type_hash = keccak256(
            b"Order(address maker,address taker,address tokenId,\
              uint256 makerAmount,uint256 takerAmount,\
              uint256 expiration,uint256 nonce,uint256 feeRateBps,\
              uint8 side,uint8 signatureType)",
        );

        let maker_addr = Address::from_str(maker)
            .map_err(|e| anyhow::anyhow!("Bad maker address: {}", e))?;
        let token_addr = Address::from_str(token_id).map_err(|_| {
            anyhow::anyhow!("token_id is not an address — use numeric encoding for CTF token IDs")
        });

        let _ = (
            &self.private_key,
            maker_amount,
            taker_amount,
            domain_separator,
            order_type_hash,
            maker_addr,
            token_addr,
        );

        Err(anyhow::anyhow!(
            "EIP-712 struct encoding for CTF token IDs requires verification \
             against current CLOB V2 source. Check rs-clob-client or CLOB V2 \
             source at github.com/Polymarket for exact ABI encoding of position IDs. \
             The domain separator above is correct. Complete struct_hash encoding below."
        ))
    }
}

/// Pre-computed HMAC key. The key schedule (expensive) is done once.
/// At fire time: clone() is O(block_size) then feed only the message.
#[derive(Clone)]
pub struct CachedHmacKey {
    inner: HmacSha256,
}

impl CachedHmacKey {
    /// Create from secret bytes. Expensive — do this once, cache in PrebuiltOrder.
    pub fn new(secret: &[u8]) -> Result<Self> {
        let inner = HmacSha256::new_from_slice(secret)
            .map_err(|e| anyhow::anyhow!("HMAC key error: {}", e))?;
        Ok(Self { inner })
    }

    /// Compute HMAC for a request. Clone is cheap (copies internal pad state).
    ///
    /// Message format: timestamp + method + path + body
    pub fn sign(&self, timestamp: &str, method: &str, path: &str, body: &[u8]) -> String {
        let mut mac = self.inner.clone();
        mac.update(timestamp.as_bytes());
        mac.update(method.as_bytes());
        mac.update(path.as_bytes());
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }
}
