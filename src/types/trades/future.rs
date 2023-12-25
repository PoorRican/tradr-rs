use crate::traits::AsDataFrame;
use crate::types::signals::Side;
use crate::types::trades::{calc_cost, Trade};
use chrono::NaiveDateTime;
use polars::frame::DataFrame;
use polars::prelude::{NamedFrom, Series};

/// Represents a potential trade to be executed
#[derive(Clone, Debug, PartialEq)]
pub struct FutureTrade {
    side: Side,
    price: f64,
    quantity: f64,
    cost: f64,
    /// The time at which the trade was identified
    point: NaiveDateTime,
}

impl FutureTrade {
    /// Create a new potential trade
    pub fn new(side: Side, price: f64, quantity: f64, point: NaiveDateTime) -> FutureTrade {
        let cost = calc_cost(price, quantity);
        FutureTrade {
            side,
            price,
            quantity,
            cost,
            point,
        }
    }
}

impl AsDataFrame for FutureTrade {
    fn as_dataframe(&self) -> DataFrame {
        DataFrame::new(vec![
            Series::new("side", vec![self.side as i32]),
            Series::new("price", vec![self.price]),
            Series::new("quantity", vec![self.quantity]),
            Series::new("cost", vec![self.cost]),
            Series::new("point", vec![self.point]),
        ])
        .unwrap()
    }
}

impl Trade for FutureTrade {
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
mod tests {
    use crate::traits::AsDataFrame;
    use crate::types::signals::Side;
    use crate::types::trades::future::FutureTrade;
    use crate::types::trades::Trade;
    use chrono::{NaiveDateTime, Utc};

    #[test]
    fn test_new() {
        let side = Side::Buy;
        let price = 1.0;
        let quantity = 2.0;
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let trade = FutureTrade::new(side, price, quantity, point);

        assert_eq!(trade.get_side(), side);
        assert_eq!(trade.get_price(), price);
        assert_eq!(trade.get_quantity(), quantity);
        assert_eq!(trade.get_cost(), price * quantity);
        assert_eq!(trade.get_point(), &point);
    }

    #[test]
    fn test_as_dataframe() {
        let side = Side::Buy;
        let price = 1.0;
        let quantity = 2.0;
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let trade = FutureTrade::new(side, price, quantity, point);
        let df = trade.as_dataframe();

        assert_eq!(df.shape(), (1, 5));
        assert_eq!(
            df.get_column_names(),
            &["side", "price", "quantity", "cost", "point"]
        );
        assert_eq!(
            df.column("side").unwrap().i32().unwrap().get(0).unwrap(),
            side as i32
        );
        assert_eq!(
            df.column("price").unwrap().f64().unwrap().get(0).unwrap(),
            price
        );
        assert_eq!(
            df.column("quantity")
                .unwrap()
                .f64()
                .unwrap()
                .get(0)
                .unwrap(),
            quantity
        );
        assert_eq!(
            df.column("cost").unwrap().f64().unwrap().get(0).unwrap(),
            trade.cost
        );
        assert_eq!(
            df.column("point")
                .unwrap()
                .datetime()
                .unwrap()
                .get(0)
                .unwrap(),
            point.timestamp_millis()
        );
    }
}
