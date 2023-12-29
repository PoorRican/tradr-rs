mod coinbase;
mod fee;
pub mod manager;

use async_trait::async_trait;

pub use coinbase::CoinbaseClient;

pub use fee::{FeeCalculator, SimplePercentageFee};

use crate::types::{Candle, ExecutedTrade, FutureTrade};

/// A minimum interface for interacting with cryptocurrency exchanges.
///
/// This interface defines methods for getting candles and submitting orders to the exchange.
#[async_trait]
pub trait BaseMarket {
    /// Returns a list of candles for the given trading pair and interval.
    ///
    /// # Arguments
    /// * `pair` - The trading pair to get candles for. This is market specific.
    /// * `interval` - The interval to get candles for. This is market specific.
    async fn get_candles(&self, pair: &str, interval: &str) -> Result<Vec<Candle>, reqwest::Error>;

    /// Submits an order to the exchange and returns the executed trade.
    ///
    /// # Arguments
    /// * `order` - A proposed order to submit to the exchange.
    /// * `product_id` - The product id to submit the order for. This is market specific.
    ///
    /// # Returns
    /// * `ExecutedTrade` - The executed trade returned by the exchange if the order was filled.
    async fn submit_order(
        &self,
        order: FutureTrade,
        product_id: String,
    ) -> Result<ExecutedTrade, reqwest::Error>;
}

/// A common interface for interacting with cryptocurrency exchanges.
#[async_trait]
pub trait Market: BaseMarket {
    /// The type of trading pair info returned by the exchange.
    ///
    /// This is used when getting the trading pair info from the exchange and not directly
    /// used for trading.
    type PairType;

    /// The type of fee calculator used by the exchange.
    ///
    /// This is used to allow implementations to use market specific fee calculators.
    type FeeCalculator;

    /// Returns a reference to the fee calculator used by the exchange.
    async fn get_fee_calculator(&self) -> Option<&dyn FeeCalculator>;

    /// Returns a list of trading pairs and their info supported by the exchange.
    async fn get_trading_pair_info(&self) -> Result<Vec<Self::PairType>, reqwest::Error>;

}
