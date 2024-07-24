mod order;

use crate::markets::coinbase::order::{CoinbaseOrderRequest, CoinbaseOrderResponse};
use crate::markets::BaseMarket;
use crate::markets::{FeeCalculator, Market, SimplePercentageFee};
use crate::types::{Candle, ExecutedTrade, FutureTrade};
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};

const BASE_URL: &str = "https://api.exchange.coinbase.com";

const VALID_INTERVALS: [[&str; 2]; 6] = [
    ["1m", "60"],
    ["5m", "300"],
    ["15m", "900"],
    ["1h", "3600"],
    ["6h", "21600"],
    ["1d", "86400"],
];

#[derive(Serialize, Deserialize, Debug)]
/// Struct that represents a trading pair on the Coinbase exchange.
pub struct TradingPairInfo {
    pub id: String,

    pub base_currency: String,

    pub quote_currency: String,

    // specifies the minimum increment for the base_currency
    pub base_increment: String,

    // specifies the min order price as well as the price increment
    pub quote_increment: String,

    pub status: String,

    // any extra information regarding the status if available.
    pub status_message: Option<String>,
}

#[derive(Clone)]
pub struct CoinbaseClient {
    api_key: String,
    api_secret: String,
    api_passphrase: String,

    client: reqwest::Client,

    enable_trades: bool,
}

impl CoinbaseClient {
    pub fn new() -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("User-Agent", "reqwest".parse().unwrap());
        headers.insert("Content-Type", "application/json".parse().unwrap());

        let client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .build()
            .unwrap();
        Self {
            api_key: "".to_string(),
            api_secret: "".to_string(),
            api_passphrase: "".to_string(),
            client,
            enable_trades: true,
        }
    }

    pub fn disable_trades(mut self) -> Self {
        self.enable_trades = false;
        self
    }
}

#[async_trait]
impl BaseMarket for CoinbaseClient {
    fn name(&self) -> &str {
        "Coinbase"
    }

    async fn get_candles(&self, pair: &str, interval: &str) -> Result<Vec<Candle>, reqwest::Error> {
        assert!(VALID_INTERVALS.iter().any(|x| x[0] == interval));

        // build url
        let url = format!(
            "{}/products/{}/candles?granularity={}",
            BASE_URL, pair, interval
        );

        // send request and parse response
        let response = self
            .client
            .get(&url)
            .send()
            .await?
            .json::<Vec<Candle>>()
            .await?;
        Ok(response)
    }

    /// Submits an order to the exchange and returns the executed trade.
    ///
    /// This method will only submit FOK orders. Therefore, if the order cannot be filled immediately,
    /// it will be cancelled.
    ///
    /// # Arguments
    /// * `order` - A proposed order to submit to the exchange.
    ///
    /// # Returns
    /// * `ExecutedTrade` - The executed trade returned by the exchange.
    /// * `reqwest::Error` - If there was an error parsing the order
    async fn submit_order(
        &self,
        order: FutureTrade,
        product_id: String,
    ) -> Result<ExecutedTrade, reqwest::Error> {
        if !self.enable_trades {
            let trade = ExecutedTrade::from_future_trade("mock".to_string(), order);
            return Ok(trade);
        }
        let request = CoinbaseOrderRequest::with_future_trade(order, product_id);

        let url = format!("{}/orders", BASE_URL);

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("cb-access-key", self.api_key.parse().unwrap());
        headers.insert(
            "cb-access-sign",
            base64::encode(self.api_secret.as_bytes()).parse().unwrap(),
        );
        headers.insert("cb-access-passphrase", self.api_passphrase.parse().unwrap());
        headers.insert("cb-access-timestamp", Utc::now().timestamp().into());

        let response = self
            .client
            .post(&url)
            .json(&request)
            .headers(headers)
            .send()
            .await?
            .json::<CoinbaseOrderResponse>()
            .await?;

        Ok(response.into())
    }
}

#[async_trait]
impl Market for CoinbaseClient {
    type PairType = TradingPairInfo;
    type FeeCalculator = SimplePercentageFee;

    async fn get_fee_calculator(&self) -> Option<&dyn FeeCalculator> {
        todo!()
    }

    async fn get_trading_pair_info(&self) -> Result<Vec<Self::PairType>, reqwest::Error> {
        let url = format!("{}/products/", BASE_URL);

        let response = self
            .client
            .get(&url)
            .send()
            .await?
            .json::<Vec<Self::PairType>>()
            .await?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use super::*;
    use crate::types::Side;

    #[test]
    fn test_new() {
        let client = CoinbaseClient::new();
        assert_eq!(client.api_key, "".to_string());
        assert_eq!(client.api_secret, "".to_string());
    }

    #[tokio::test]
    async fn test_get_trading_pair_info() {
        let client = CoinbaseClient::new();
        let info = client.get_trading_pair_info().await;
        assert!(info.is_ok());
    }

    #[tokio::test]
    async fn test_get_candles() {
        let client = CoinbaseClient::new();
        let candles = client.get_candles("BTC-USD", "1m").await.unwrap();
        assert_eq!(candles.len(), 300);
    }

    #[tokio::test]
    async fn test_submit_order() {
        let product_id = "BTC-USD".to_string();
        let client = CoinbaseClient::new();
        let order = FutureTrade::new(Side::Buy, dec!(1.0), dec!(1.0), Utc::now().naive_utc());
        let response = client.submit_order(order, product_id).await;

        // TODO: use a small trade or testnet to make this work
        // we expect this to fail since the endpoint requires authentication
        assert!(response.is_err());
    }
}
