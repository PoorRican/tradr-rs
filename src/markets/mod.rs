mod fee;
mod coinbase;

use async_trait::async_trait;
pub use fee::{SimplePercentageFee, FeeCalculator};
pub use coinbase::CoinbaseClient;

use crate::types::{Candle, ExecutedTrade, FutureTrade};

#[async_trait]
pub trait Market {
    type PairType;
    type FeeCalculator;

    async fn get_fee_calculator(&self) -> Option<&dyn FeeCalculator>;
    async fn get_trading_pair_info(&self) -> Result<Vec<Self::PairType>, reqwest::Error>;
    async fn get_candles(&self,
                   pair: &str,
                   interval: &str,
    ) -> Result<Vec<Candle>, reqwest::Error>;

    async fn submit_order(&self, order: FutureTrade) -> Result<ExecutedTrade, reqwest::Error>;
}