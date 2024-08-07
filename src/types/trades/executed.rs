use crate::types::signals::Side;
use crate::types::trades::future::FutureTrade;
use crate::types::trades::{calc_notional_value, Trade};
use chrono::NaiveDateTime;
use rust_decimal::Decimal;

/// Represents a trade that has been executed on the market
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutedTrade {
    order_id: String,
    side: Side,
    price: Decimal,
    quantity: Decimal,
    notional_value: Decimal,
    timestamp: NaiveDateTime,
}

impl ExecutedTrade {
    pub fn new(
        order_id: String,
        side: Side,
        price: Decimal,
        quantity: Decimal,
        notional_value: Decimal,
        timestamp: NaiveDateTime,
    ) -> Self {
        ExecutedTrade {
            order_id,
            side,
            price,
            quantity,
            notional_value,
            timestamp,
        }
    }

    /// This is a constructor that internally calculates the notional value of the trade
    ///
    /// This is meant primarily for testing purposes and would not be used for parsing
    /// actual executed trades.
    pub fn with_calculated_notional(
        order_id: String,
        side: Side,
        price: Decimal,
        quantity: Decimal,
        timestamp: NaiveDateTime,
    ) -> ExecutedTrade {
        let notional_value = calc_notional_value(price, quantity);
        ExecutedTrade {
            order_id,
            side,
            price,
            quantity,
            notional_value,
            timestamp,
        }
    }

    pub fn from_future_trade(order_id: String, trade: FutureTrade) -> ExecutedTrade {
        ExecutedTrade {
            order_id,
            side: trade.get_side(),
            price: trade.get_price(),
            quantity: trade.get_quantity(),
            notional_value: trade.get_notional_value(),
            timestamp: trade.get_timestamp().clone(),
        }
    }

    pub fn get_order_id(&self) -> &String {
        &self.order_id
    }
}

impl Trade for ExecutedTrade {
    fn get_side(&self) -> Side {
        self.side
    }

    fn get_price(&self) -> Decimal {
        self.price
    }

    fn get_quantity(&self) -> Decimal {
        self.quantity
    }

    fn get_notional_value(&self) -> Decimal {
        self.notional_value
    }

    fn get_timestamp(&self) -> &NaiveDateTime {
        &self.timestamp
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::types::signals::Side;
    use crate::types::trades::calc_notional_value;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    #[test]
    fn test_new() {
        let order_id = "order123".to_string();
        let execution_side = Side::Buy;
        let execution_price = dec!(100.50);
        let execution_quantity = dec!(10.0);
        let notional_value = dec!(1005.00);
        let execution_timestamp = Utc::now().naive_utc();

        let trade = ExecutedTrade::new(
            order_id.clone(),
            execution_side,
            execution_price,
            execution_quantity,
            notional_value,
            execution_timestamp.clone(),
        );

        assert_eq!(trade.order_id, order_id);
        assert_eq!(trade.side, execution_side);
        assert_eq!(trade.price, execution_price);
        assert_eq!(trade.quantity, execution_quantity);
        assert_eq!(trade.notional_value, notional_value);
        assert_eq!(trade.timestamp, execution_timestamp);
    }

    #[test]
    fn test_new_with_calculated_notional() {
        let order_id = "order456".to_string();
        let execution_side = Side::Sell;
        let execution_price = dec!(50.25);
        let execution_quantity = dec!(5.0);
        let notional_value = calc_notional_value(execution_price, execution_quantity);
        let execution_timestamp =
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let trade = ExecutedTrade::with_calculated_notional(
            order_id.clone(),
            execution_side,
            execution_price,
            execution_quantity,
            execution_timestamp.clone(),
        );

        assert_eq!(trade.order_id, order_id);
        assert_eq!(trade.side, execution_side);
        assert_eq!(trade.price, execution_price);
        assert_eq!(trade.quantity, execution_quantity);
        assert_eq!(trade.notional_value, notional_value);
        assert_eq!(trade.timestamp, execution_timestamp);
    }

    #[test]
    fn test_from_future_trade() {
        let order_id = "order789".to_string();
        let execution_side = Side::Buy;
        let execution_price = dec!(75.00);
        let execution_quantity = dec!(8.0);
        let notional_value = calc_notional_value(execution_price, execution_quantity);
        let execution_timestamp =
            NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let future_trade = FutureTrade::new(
            execution_side,
            execution_price,
            execution_quantity,
            execution_timestamp.clone(),
        );

        let executed_trade = ExecutedTrade::from_future_trade(order_id.clone(), future_trade);

        assert_eq!(executed_trade.order_id, order_id);
        assert_eq!(executed_trade.side, execution_side);
        assert_eq!(executed_trade.price, execution_price);
        assert_eq!(executed_trade.quantity, execution_quantity);
        assert_eq!(executed_trade.notional_value, notional_value);
        assert_eq!(executed_trade.timestamp, execution_timestamp);
    }
}
