use crate::traits::AsDataFrame;
use crate::types::signals::Side;
use crate::types::trades::future::FutureTrade;
use crate::types::trades::{calc_cost, Trade};
use chrono::NaiveDateTime;
use polars::frame::DataFrame;
use polars::prelude::{NamedFrom, Series};

/// Represents a trade that has been executed on the market
pub struct ExecutedTrade {
    id: String,
    side: Side,
    price: f64,
    quantity: f64,
    cost: f64,
    point: NaiveDateTime,
}

impl ExecutedTrade {
    pub fn new(
        id: String,
        side: Side,
        price: f64,
        quantity: f64,
        cost: f64,
        point: NaiveDateTime,
    ) -> Self {
        ExecutedTrade {
            id,
            side,
            price,
            quantity,
            cost,
            point,
        }
    }

    /// This is a constructor that internally calculates the cost of the trade
    ///
    /// This is meant primarily for testing purposes and would not be used for parsing
    /// actual executed trades.
    pub fn new_without_cost(
        id: String,
        side: Side,
        price: f64,
        quantity: f64,
        point: NaiveDateTime,
    ) -> ExecutedTrade {
        let cost = calc_cost(price, quantity);
        ExecutedTrade {
            id,
            side,
            price,
            quantity,
            cost,
            point,
        }
    }
    pub fn with_future_trade(id: String, trade: FutureTrade) -> ExecutedTrade {
        ExecutedTrade {
            id,
            side: trade.get_side(),
            price: trade.get_price(),
            quantity: trade.get_quantity(),
            cost: trade.get_cost(),
            point: trade.get_point().clone(),
        }
    }

    pub fn get_id(&self) -> &String {
        &self.id
    }

    pub fn from_row(row: &DataFrame) -> Self {
        assert_eq!(row.height(), 1);
        assert_eq!(
            row.get_column_names(),
            &["id", "side", "price", "quantity", "cost", "point"]
        );
        let id = row.column("id").unwrap().str().unwrap().get(0).unwrap();
        let side = Side::from(row.column("side").unwrap().i8().unwrap().get(0).unwrap());
        let price = row.column("price").unwrap().f64().unwrap().get(0).unwrap();
        let quantity = row
            .column("quantity")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        let cost = row.column("cost").unwrap().f64().unwrap().get(0).unwrap();
        let point = NaiveDateTime::from_timestamp_millis(
            row.column("point")
                .unwrap()
                .datetime()
                .unwrap()
                .get(0)
                .unwrap(),
        )
        .unwrap();
        ExecutedTrade::new(id.to_string(), side, price, quantity, cost, point)
    }
}

impl AsDataFrame for ExecutedTrade {
    fn as_dataframe(&self) -> DataFrame {
        DataFrame::new(vec![
            Series::new("id", vec![self.id.clone()]),
            Series::new("side", vec![self.side as i8]),
            Series::new("price", vec![self.price]),
            Series::new("quantity", vec![self.quantity]),
            Series::new("cost", vec![self.cost]),
            Series::new("point", vec![self.point]),
        ])
        .unwrap()
    }
}

impl Trade for ExecutedTrade {
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
    use super::*;
    use crate::traits::AsDataFrame;
    use crate::types::signals::Side;
    use crate::types::trades::calc_cost;
    use chrono::Utc;

    #[test]
    fn test_new() {
        let id = "id".to_string();
        let side = Side::Buy;
        let price = 1.0;
        let quantity = 2.0;
        let cost = 3.0;
        let point = Utc::now().naive_utc();

        let trade = ExecutedTrade::new(id.clone(), side, price, quantity, cost, point.clone());

        assert_eq!(trade.id, id);
        assert_eq!(trade.side, side);
        assert_eq!(trade.price, price);
        assert_eq!(trade.quantity, quantity);
        assert_eq!(trade.cost, cost);
        assert_eq!(trade.point, point);
    }

    #[test]
    fn test_new_without_cost() {
        let id = "id".to_string();
        let side = Side::Buy;
        let price = 1.0;
        let quantity = 2.0;
        let cost = calc_cost(price, quantity);
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let trade =
            ExecutedTrade::new_without_cost(id.clone(), side, price, quantity, point.clone());

        assert_eq!(trade.id, id);
        assert_eq!(trade.side, side);
        assert_eq!(trade.price, price);
        assert_eq!(trade.quantity, quantity);
        assert_eq!(trade.cost, cost);
        assert_eq!(trade.point, point);
    }

    #[test]
    fn test_with_future_trade() {
        let id = "id".to_string();
        let side = Side::Buy;
        let price = 1.0;
        let quantity = 2.0;
        let cost = calc_cost(price, quantity);
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let future_trade = FutureTrade::new(side, price, quantity, point.clone());

        let failed_trade = ExecutedTrade::with_future_trade(id.clone(), future_trade);

        assert_eq!(failed_trade.id, id);
        assert_eq!(failed_trade.side, side);
        assert_eq!(failed_trade.price, price);
        assert_eq!(failed_trade.quantity, quantity);
        assert_eq!(failed_trade.cost, cost);
        assert_eq!(failed_trade.point, point);
    }

    #[test]
    fn test_as_dataframe() {
        let id = "id".to_string();
        let side = Side::Buy;
        let price = 1.0;
        let quantity = 2.0;
        let cost = 3.0;
        let point = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let trade = ExecutedTrade {
            id: id.clone(),
            side,
            price,
            quantity,
            cost,
            point,
        };

        let df = trade.as_dataframe();
        assert_eq!(df.shape(), (1, 6));
        assert_eq!(
            df.get_column_names(),
            &["id", "side", "price", "quantity", "cost", "point"]
        );
        assert_eq!(
            df.column("side").unwrap().i8().unwrap().get(0).unwrap(),
            side as i8
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
            cost
        );
        assert_eq!(df.column("id").unwrap().str().unwrap().get(0).unwrap(), id);
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

    #[test]
    fn test_from_row() {
        use polars::prelude::*;

        let time = Utc::now().naive_utc();

        let row = df![
            "id" => ["id"],
            "side" => [Side::Buy as i8],
            "price" => [1.0],
            "quantity" => [2.0],
            "cost" => [3.0],
            "point" => [time]
        ]
        .unwrap();
        let trade = ExecutedTrade::from_row(&row);

        assert_eq!(trade.id, "id");
        assert_eq!(trade.side, Side::Buy);
        assert_eq!(trade.price, 1.0);
        assert_eq!(trade.quantity, 2.0);
        assert_eq!(trade.cost, 3.0);
        assert_eq!(trade.point.timestamp_millis(), time.timestamp_millis());
    }
}
