use crate::types::reason_code::ReasonCode;
use crate::types::signals::Side;
use crate::types::trades::future::FutureTrade;
use crate::types::trades::{calc_notional_value, Trade};
use chrono::NaiveDateTime;
use polars::prelude::{NamedFrom, Series};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

/// Represents a trade that has been rejected by the market or otherwise failed
pub struct FailedTrade {
    reason: ReasonCode,
    side: Side,
    price: Decimal,
    quantity: Decimal,
    cost: Decimal,
    point: NaiveDateTime,
}

impl FailedTrade {
    pub fn new(
        reason: ReasonCode,
        side: Side,
        price: Decimal,
        quantity: Decimal,
        point: NaiveDateTime,
    ) -> FailedTrade {
        let cost = calc_notional_value(price, quantity);
        FailedTrade {
            reason,
            side,
            price,
            quantity,
            cost,
            point,
        }
    }

    pub fn with_future_trade(reason: ReasonCode, trade: FutureTrade) -> FailedTrade {
        FailedTrade {
            reason,
            side: trade.get_side(),
            price: trade.get_price(),
            quantity: trade.get_quantity(),
            cost: trade.get_notional_value(),
            point: trade.get_timestamp().clone(),
        }
    }
}

impl Trade for FailedTrade {
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
        self.cost
    }

    fn get_timestamp(&self) -> &NaiveDateTime {
        &self.point
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
        let reason = ReasonCode::Unknown;
        let side = Side::Buy;
        let price = dec!(1.0);
        let quantity = dec!(2.0);
        let cost = calc_notional_value(price, quantity);
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let failed_trade = FailedTrade::new(reason, side, price, quantity, point.clone());

        assert_eq!(failed_trade.reason, reason);
        assert_eq!(failed_trade.side, side);
        assert_eq!(failed_trade.price, price);
        assert_eq!(failed_trade.quantity, quantity);
        assert_eq!(failed_trade.cost, cost);
        assert_eq!(failed_trade.point, point);
    }

    /// Test the constructor for `FailedTrade`
    #[test]
    fn test_with_future_trade() {
        let reason = ReasonCode::Unknown;
        let side = Side::Buy;
        let price = dec!(1.0);
        let quantity = dec!(2.0);
        let cost = calc_notional_value(price, quantity);
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let future_trade = FutureTrade::new(side, price, quantity, point.clone());

        let failed_trade = FailedTrade::with_future_trade(reason, future_trade);

        assert_eq!(failed_trade.reason, reason);
        assert_eq!(failed_trade.side, side);
        assert_eq!(failed_trade.price, price);
        assert_eq!(failed_trade.quantity, quantity);
        assert_eq!(failed_trade.cost, cost);
        assert_eq!(failed_trade.point, point);
    }
}
