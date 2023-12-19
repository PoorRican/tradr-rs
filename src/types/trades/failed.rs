use chrono::NaiveDateTime;
use polars::frame::DataFrame;
use polars::prelude::{NamedFrom, Series};
use crate::types::reason_code::ReasonCode;
use crate::types::signals::Side;
use crate::types::trades::future::FutureTrade;
use crate::types::trades::{calc_cost, Trade};
use crate::traits::AsDataFrame;

/// Represents a trade that has been rejected by the market or otherwise failed
pub struct FailedTrade {
    reason: ReasonCode,
    side: Side,
    price: f64,
    quantity: f64,
    cost: f64,
    point: NaiveDateTime,
}

impl FailedTrade {
    pub fn new(
        reason: ReasonCode,
        side: Side,
        price: f64,
        quantity: f64,
        point: NaiveDateTime,
    ) -> FailedTrade {
        let cost = calc_cost(price, quantity);
        FailedTrade {
            reason,
            side,
            price,
            quantity,
            cost,
            point
        }
    }

    pub fn with_future_trade(
        reason: ReasonCode,
        trade: FutureTrade,
    ) -> FailedTrade {
        FailedTrade {
            reason,
            side: trade.get_side(),
            price: trade.get_price(),
            quantity: trade.get_quantity(),
            cost: trade.get_cost(),
            point: trade.get_point().clone()
        }
    }
}

impl AsDataFrame for FailedTrade {
    fn as_dataframe(&self) -> DataFrame {
        DataFrame::new(vec![
            Series::new("side", vec![self.side as i32]),
            Series::new("price", vec![self.price]),
            Series::new("quantity", vec![self.quantity]),
            Series::new("cost", vec![self.cost]),
            Series::new("reason", vec![self.reason as i32]),
            Series::new("point", vec![self.point]),
        ]).unwrap()
    }
}

impl Trade for FailedTrade {
    fn get_side(&self) -> Side {
        self.side
    }

    fn get_price(&self) -> f64 {
        self.price
    }

    fn get_quantity(&self) -> f64 {
        self.quantity
    }

    fn get_cost(&self) -> f64 {
        self.cost
    }

    fn get_point(&self) -> &NaiveDateTime {
        &self.point
    }
}


#[cfg(test)]
mod test {
    use chrono::Utc;
    use super::*;
    use crate::types::signals::Side;
    use crate::types::trades::calc_cost;
    use crate::traits::AsDataFrame;

    #[test]
    fn test_new() {
        let reason = ReasonCode::Unknown;
        let side = Side::Buy;
        let price = 1.0;
        let quantity = 2.0;
        let cost = calc_cost(price, quantity);
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let failed_trade = FailedTrade::new(
            reason,
            side,
            price,
            quantity,
            point.clone(),
        );

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
        let price = 1.0;
        let quantity = 2.0;
        let cost = calc_cost(price, quantity);
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let future_trade = FutureTrade::new(
            side,
            price,
            quantity,
            point.clone(),
        );

        let failed_trade = FailedTrade::with_future_trade(
            reason,
            future_trade,
        );

        assert_eq!(failed_trade.reason, reason);
        assert_eq!(failed_trade.side, side);
        assert_eq!(failed_trade.price, price);
        assert_eq!(failed_trade.quantity, quantity);
        assert_eq!(failed_trade.cost, cost);
        assert_eq!(failed_trade.point, point);
    }

    /// Test the `as_dataframe` method for `FailedTrade`
    #[test]
    fn test_as_dataframe() {
        let reason = ReasonCode::Unknown;
        let side = Side::Buy;
        let price = 1.0;
        let quantity = 2.0;
        let cost = 3.0;
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let trade = FailedTrade {
            reason,
            side,
            price,
            quantity,
            cost,
            point,
        };

        let df = trade.as_dataframe();
        assert_eq!(df.shape(), (1, 6));
        assert_eq!(df.get_column_names(), &["side", "price", "quantity", "cost", "reason", "point"]);
        assert_eq!(df.column("side").unwrap().i32().unwrap().get(0).unwrap(), side as i32);
        assert_eq!(df.column("price").unwrap().f64().unwrap().get(0).unwrap(), price);
        assert_eq!(df.column("quantity").unwrap().f64().unwrap().get(0).unwrap(), quantity);
        assert_eq!(df.column("cost").unwrap().f64().unwrap().get(0).unwrap(), cost);
        assert_eq!(df.column("reason").unwrap().i32().unwrap().get(0).unwrap(), reason as i32);
        assert_eq!(
            df.column("point")
                .unwrap()
                .datetime()
                .unwrap()
                .last()
                .unwrap(),
            point.timestamp_millis());
    }
}