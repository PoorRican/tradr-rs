use polars::frame::DataFrame;
use polars::prelude::{NamedFrom, Series};
use crate::types::signals::Side;
use crate::types::time::Timestamp;
use crate::types::trades::{calc_cost, Trade};
use crate::traits::AsDataFrame;

/// Represents a potential trade to be executed
pub struct FutureTrade {
    side: Side,
    price: f64,
    quantity: f64,
    cost: f64,
    /// The time at which the trade was identified
    point: Timestamp
}

impl FutureTrade {
    /// Create a new potential trade
    pub fn new(
        side: Side,
        price: f64,
        quantity: f64,
        point: Timestamp,
    ) -> FutureTrade {
        let cost = calc_cost(price, quantity);
        FutureTrade {
            side,
            price,
            quantity,
            cost,
            point
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
            Series::new("point", vec![self.point.timestamp()]),
        ]).unwrap()
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

    fn get_point(&self) -> &Timestamp {
        &self.point
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use crate::types::signals::Side;
    use crate::types::trades::future::FutureTrade;
    use crate::types::trades::Trade;
    use crate::traits::AsDataFrame;

    #[test]
    fn test_new() {
        let side = Side::Buy;
        let price = 1.0;
        let quantity = 2.0;
        let point = Utc::now();

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
        let point = Utc::now();

        let trade = FutureTrade::new(side, price, quantity, point);
        let df = trade.as_dataframe();

        assert_eq!(df.shape(), (1, 5));
        assert_eq!(df.get_column_names(), &["side", "price", "quantity", "cost", "point"]);
        assert_eq!(df.column("side").unwrap().i32().unwrap().get(0).unwrap(), side as i32);
        assert_eq!(df.column("price").unwrap().f64().unwrap().get(0).unwrap(), price);
        assert_eq!(df.column("quantity").unwrap().f64().unwrap().get(0).unwrap(), quantity);
        assert_eq!(df.column("cost").unwrap().f64().unwrap().get(0).unwrap(), trade.cost);
        assert_eq!(df.column("point").unwrap().i64().unwrap().get(0).unwrap(), point.timestamp());
    }
}