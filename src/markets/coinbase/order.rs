use chrono::NaiveDateTime;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::types::{ExecutedTrade, FutureTrade, Side, Trade};

#[derive(Debug, PartialEq, Clone)]
pub enum CoinbaseMarketOrderType {
    Limit,
    Market,
    Stop,
}

impl Serialize for CoinbaseMarketOrderType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CoinbaseMarketOrderType::Limit => serializer.serialize_str("limit"),
            CoinbaseMarketOrderType::Market => serializer.serialize_str("market"),
            CoinbaseMarketOrderType::Stop => serializer.serialize_str("stop"),
        }
    }
}

impl<'de> Deserialize<'de> for CoinbaseMarketOrderType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "limit" => Ok(CoinbaseMarketOrderType::Limit),
            "market" => Ok(CoinbaseMarketOrderType::Market),
            "stop" => Ok(CoinbaseMarketOrderType::Stop),
            _ => Err(Error::custom("Unexpected value for CoinbaseMarketType")),
        }
    }
}

/// Coinbase order request.
///
/// # Order Types
/// Coinbase defaults to a limit order.
///
/// - Limit: Requires specifying both `price` and `size`. Where `size` is the number of cryptocurrency to buy or sell and
///     `price` is the price per unit of cryptocurrency. A limit order will only be executed at the specified price or better.
///     If the order is not fully filled, it will become part of the open order book until filled by another incoming order or
///     canceled by the user.
///
/// - Market: A market order provides no pricing guarantees and are subject to the market. They just provide an easier way
///     to buy or sell cryptocurrency at the current market price. Market orders execute immediately and not part of the market
///     order book, and are therefore always considered "takers". When placing a market order you can specify funds and/or size.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CoinbaseOrderRequest {
    pub profile_id: Option<String>,
    pub r#type: CoinbaseMarketOrderType,
    pub side: Side,
    pub product_id: String,

    /// Price for unit of cryptocurrency. Required if type is limit or stop limit.
    pub price: Option<f64>,

    /// Amount of cryptocurrency to buy or sell.
    ///
    /// Required for limit and stop limit orders, as well as market sells.
    pub size: Option<f64>,

    /// Amount of quote currency to use. Required for market buys.
    pub funds: Option<f64>,

    /// Possible values: GTC, GTT, IOC, or FOK
    ///
    /// If not specified, Coinbase's default is GTC.
    /// However, we will default to FOK.
    pub time_in_force: Option<String>,

    /// Possible values 'min', 'hour', 'day'.
    ///
    /// Requires `time_in_force` to be GTT.
    pub cancel_after: Option<String>,

    /// Post only flag.
    ///
    /// Invalid when time_in_force is IOC or FOK.
    pub post_only: Option<bool>,

    /// Optional order user id.
    ///
    /// Must be a UUIDv4
    pub client_oid: Option<String>,

    /// Use this to show how much to show
    pub max_floor: Option<String>,
}

impl CoinbaseOrderRequest {
    pub fn new_limit_order(
        side: Side,
        product_id: String,
        price: f64,
        size: f64) -> Self
    {
        Self {
            profile_id: None,
            r#type: CoinbaseMarketOrderType::Limit,
            side,
            product_id,
            price: Some(price),
            size: Some(size),
            funds: None,
            time_in_force: Some("FOK".to_string()),
            cancel_after: None,
            post_only: None,
            client_oid: None,
            max_floor: None,
        }
    }

    pub fn with_future_trade(
        trade: FutureTrade,
        product_id: String,
    ) -> Self {
        Self::new_limit_order(
            trade.get_side(),
            product_id,
            trade.get_price(),
            trade.get_quantity(),
        )
    }

    pub fn set_client_oid(mut self, client_oid: String) -> Self {
        self.client_oid = Some(client_oid);
        self
    }
}

/// Coinbase order response.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CoinbaseOrderResponse {
    /// Order id.
    pub id: String,

    /// Price per unit of base currency.
    pub price: f64,

    /// Amount of base currency to buy or sell.
    pub size: f64,

    /// Book the order belongs to.
    pub product_id: String,

    /// `profile_id` that placed the order
    pub profile_id: Option<String>,

    /// Possible values are `buy` or `sell`.
    pub side: Side,

    /// amount of quote currency to spend (for market orders)
    pub funds: Option<f64>,

    /// funds with fees
    pub specified_funds: Option<f64>,

    /// Possible values are `limit`, `market`, or `stop`.
    pub r#type: CoinbaseMarketOrderType,

    /// Possible values are `GTC`, `GTT`, `IOC`, or `FOK`.
    pub time_in_force: Option<String>,

    /// timestamp at which the order expires
    pub expire_time: Option<String>,

    /// if true, forces order to be `maker` only
    pub post_only: Option<bool>,

    /// timestamp at which order was created
    pub created_at: String,

    /// timestamp at which order was filled
    pub done_at: Option<String>,

    /// reason why the order was done (filled, rejected, or otherwise)
    pub done_reason: Option<String>,

    /// reason order was rejected by engine
    pub reject_reason: Option<String>,

    /// fees paid by current order amount
    pub fill_fees: f64,

    /// amount (in base currency) of current order filled
    pub filled_size: f64,

    pub executed_value: f64,

    /// Possible values: [open, pending, rejected, done, active, received, all]
    pub status: String,

    /// true if funds have been exchanged and settled
    pub settled: bool,

    /// possible values [loss, entry]
    pub stop: Option<String>,

    pub funding_amount: Option<String>,

    /// client supplied order id. Will be a UUIDv4
    pub client_oid: Option<String>,

    /// market type where order was traded
    pub market_type: Option<String>,

    /// maximum visible quantity for iceberg order
    pub max_floor: Option<String>,

    /// order id for the visible order for iceberg order
    pub secondary_order_id: Option<String>,
}

impl Into<ExecutedTrade> for CoinbaseOrderResponse {
    fn into(self) -> ExecutedTrade {
        let point = NaiveDateTime::parse_from_str(&self.created_at, "%Y-%m-%dT%H:%M:%S%.fZ").unwrap();
        ExecutedTrade::new(
            self.id.to_string(),
            self.side,
            self.price,
            self.size,
            point
        )
    }
}

#[cfg(test)]
mod order_type_tests {
    /// Tests that the `CoinbaseMarketOrderType` enum can be serialized and deserialized correctly.
    #[test]
    fn test_serialize_limit_order_type() {
        let order_type = super::CoinbaseMarketOrderType::Limit;
        let serialized = serde_json::to_string(&order_type).unwrap();
        assert_eq!(serialized, "\"limit\"");

        let order_type = super::CoinbaseMarketOrderType::Market;
        let serialized = serde_json::to_string(&order_type).unwrap();
        assert_eq!(serialized, "\"market\"");

        let order_type = super::CoinbaseMarketOrderType::Stop;
        let serialized = serde_json::to_string(&order_type).unwrap();
        assert_eq!(serialized, "\"stop\"");
    }

    #[test]
    fn test_deserialize_limit_order_type() {
        let order_type =
            serde_json::from_str::<super::CoinbaseMarketOrderType>("\"limit\"").unwrap();
        assert_eq!(order_type, super::CoinbaseMarketOrderType::Limit);

        let order_type =
            serde_json::from_str::<super::CoinbaseMarketOrderType>("\"market\"").unwrap();
        assert_eq!(order_type, super::CoinbaseMarketOrderType::Market);

        let order_type =
            serde_json::from_str::<super::CoinbaseMarketOrderType>("\"stop\"").unwrap();
        assert_eq!(order_type, super::CoinbaseMarketOrderType::Stop);
    }
}

#[cfg(test)]
mod order_request_tests {
    use crate::markets::coinbase::order::CoinbaseOrderRequest;
    use crate::types::FutureTrade;

    #[test]
    fn test_new_limit_order() {
        let order = CoinbaseOrderRequest::new_limit_order(
            super::Side::Buy,
            "BTC-USD".to_string(),
            100.0,
            1.0,
        );
        assert_eq!(order.side, super::Side::Buy);
        assert_eq!(order.product_id, "BTC-USD");
        assert_eq!(order.price, Some(100.0));
        assert_eq!(order.size, Some(1.0));
        assert_eq!(order.funds, None);
        assert_eq!(order.time_in_force, Some("FOK".to_string()));
        assert_eq!(order.cancel_after, None);
        assert_eq!(order.post_only, None);
        assert_eq!(order.client_oid, None);
        assert_eq!(order.max_floor, None);
    }

    #[test]
    fn test_with_future_trade() {
        let price = 100.0;
        let quantity = 1.0;
        let product_id = "BTC-USD".to_string();

        // try with a buy order
        let trade = FutureTrade::new(
            super::Side::Buy,
            price,
            quantity,
            chrono::Utc::now().naive_utc(),
        );

        let order = CoinbaseOrderRequest::with_future_trade(
            trade,
            product_id.clone(),
        );

        assert_eq!(order.side, super::Side::Buy);
        assert_eq!(order.product_id, product_id);
        assert_eq!(order.price, Some(price));
        assert_eq!(order.size, Some(quantity));

        // try with a sell order
        let trade = FutureTrade::new(
            super::Side::Sell,
            price,
            quantity,
            chrono::Utc::now().naive_utc(),
        );

        let order = CoinbaseOrderRequest::with_future_trade(
            trade,
            product_id.clone(),
        );

        assert_eq!(order.side, super::Side::Sell);
        assert_eq!(order.product_id, product_id);
        assert_eq!(order.price, Some(price));
        assert_eq!(order.size, Some(quantity));
    }

    #[test]
    fn test_set_client_oid() {
        let order = CoinbaseOrderRequest::new_limit_order(
            super::Side::Buy,
            "BTC-USD".to_string(),
            100.0,
            1.0,
        );
        let order = order.set_client_oid("test".to_string());
        assert_eq!(order.client_oid, Some("test".to_string()));
    }
}

#[cfg(test)]
mod order_response_tests {
    use chrono::NaiveDateTime;
    use crate::types::{ExecutedTrade, Trade};
    use super::{CoinbaseMarketOrderType, CoinbaseOrderResponse};
    use crate::types::Side;

    #[test]
    fn test_order_response_into_executed_trade() {
        let order = CoinbaseOrderResponse {
            id: "uuid".to_string(),
            price: 100.0,
            size: 1.0,
            product_id: "BTC-USD".to_string(),
            profile_id: None,
            side: Side::Buy,
            funds: None,
            specified_funds: None,
            r#type: CoinbaseMarketOrderType::Limit,
            time_in_force: None,
            expire_time: None,
            post_only: None,
            created_at: "2021-01-01T00:00:00.000Z".to_string(),
            done_at: None,
            done_reason: None,
            reject_reason: None,
            fill_fees: 0.0,
            filled_size: 0.0,
            executed_value: 0.0,
            status: "open".to_string(),
            settled: false,
            stop: None,
            funding_amount: None,
            client_oid: None,
            market_type: None,
            max_floor: None,
            secondary_order_id: None,
        };
        let trade: ExecutedTrade = order.clone().into();
        assert_eq!(trade.get_id(), &order.id.to_string());
        assert_eq!(trade.get_side(), order.side);
        assert_eq!(trade.get_price(), order.price);
        assert_eq!(trade.get_quantity(), order.size);
        assert_eq!(*trade.get_point(), NaiveDateTime::parse_from_str(&order.created_at, "%Y-%m-%dT%H:%M:%S%.fZ").unwrap());
    }
}
