use crate::types::signals::Side;
use crate::types::trades::{calc_notional_value, Trade};
use chrono::NaiveDateTime;
use rust_decimal::Decimal;

/// Represents a potential trade to be executed
#[derive(Clone, Debug, PartialEq)]
pub struct FutureTrade {
    side: Side,
    price: Decimal,
    quantity: Decimal,
    cost: Decimal,
    /// The time at which the trade was identified
    point: NaiveDateTime,
}

impl FutureTrade {
    /// Create a new potential trade
    pub fn new(side: Side, price: Decimal, quantity: Decimal, point: NaiveDateTime) -> FutureTrade {
        let cost = calc_notional_value(price, quantity);
        FutureTrade {
            side,
            price,
            quantity,
            cost,
            point,
        }
    }

    pub fn new_with_nominal(side: Side, price: Decimal, quantity: Decimal, cost: Decimal, point: NaiveDateTime) -> FutureTrade {
        FutureTrade {
            side,
            price,
            quantity,
            cost,
            point,
        }
    }
}

impl Trade for FutureTrade {
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
mod tests {
    use crate::types::signals::Side;
    use crate::types::trades::future::FutureTrade;
    use crate::types::trades::Trade;
    use chrono::{NaiveDateTime, Utc};
    use rust_decimal_macros::dec;

    #[test]
    fn test_new() {
        let side = Side::Buy;
        let price = dec!(1.0);
        let quantity = dec!(2.0);
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let trade = FutureTrade::new(side, price, quantity, point);

        assert_eq!(trade.get_side(), side);
        assert_eq!(trade.get_price(), price);
        assert_eq!(trade.get_quantity(), quantity);
        assert_eq!(trade.get_notional_value(), price * quantity);
        assert_eq!(trade.get_timestamp(), &point);
    }
}
