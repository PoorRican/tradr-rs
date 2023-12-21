use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use crate::markets::{FeeCalculator, Market, SimplePercentageFee};
use crate::types::{Candle, ExecutedTrade, FutureTrade};

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


pub struct CoinbaseClient {
    api_key: String,
    api_secret: String,

    client: reqwest::Client,
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
            client,
        }
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

        let response = self.client.get(&url)
            .send()
            .await?
            .json::<Vec<Self::PairType>>().await?;
        Ok(response)
    }

    async fn get_candles(&self,
                   pair: &str,
                   interval: &str,
    ) -> Result<Vec<Candle>, reqwest::Error> {
        assert!(VALID_INTERVALS.iter().any(|x| x[0] == interval));

        // build url
        let url = format!("{}/products/{}/candles?granularity={}", BASE_URL, pair, interval);

        // send request and parse response
        let response = self.client.get(&url)
            .send()
            .await?
            .json::<Vec<Candle>>()
            .await?;
        Ok(response)
    }

    async fn submit_order(&self, order: FutureTrade) -> Result<ExecutedTrade, reqwest::Error> {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

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
}