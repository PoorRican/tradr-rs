use chrono::NaiveDateTime;
use polars::frame::DataFrame;
use polars::prelude::*;
use crate::portfolio::Portfolio;
use crate::types::signals::Side;

pub trait PositionHandlers {
    fn set_as_open_position(&mut self);

    fn get_open_positions(&self) -> Option<DataFrame>;

    fn select_open_positions(&self, price: f64) -> Option<DataFrame>;
}

impl PositionHandlers for Portfolio {
    /// Set the last executed buy trade as an open position
    ///
    /// This is intended to be called when the timeout has been reached when waiting for a sell trade.
    /// If a sell trade does not occur within the timeout, the last executed buy trade is set as an open position,
    /// that way another buy trade may be executed.
    fn set_as_open_position(&mut self) {
        // get last row
        let last_row = self.executed_trades
            .sort(["point"], false, true)
            .unwrap()
            .tail(Some(1));


        // extract side
        let side = last_row
            .column("side")
            .unwrap()
            .get(0)
            .unwrap();

        // if side is buy, add to timestamp `open_positions`
        if let AnyValue::Int32(inner) = side {
            match inner.into() {
                Side::Buy => {
                    let millis = last_row
                        .column("point")
                        .unwrap()
                        .datetime()
                        .unwrap()
                        .get(0)
                        .unwrap();
                    let point = NaiveDateTime::from_timestamp_millis(millis).unwrap();
                    self.open_positions.push(point);
                }
                _ => {}
            }
        }
    }

    /// Get open positions
    ///
    /// This returns the rows in `executed_trades` who's timestamps occur in `open_positions`
    ///
    /// # Returns
    /// A dataframe of trades corresponding to open positions.
    /// If there are no open positions, `None` is returned.
    fn get_open_positions(&self) -> Option<DataFrame> {
        if self.open_positions.is_empty() {
            return None;
        }

        // create a mask for all rows in `executed_trades` who's timestamps occur in `open_positions`
        let mask = self.executed_trades
            .column("point").unwrap()
            .datetime().unwrap()
            .into_iter()
            .map(|x|
                if let Some(t) = x {
                    self.open_positions.contains(&NaiveDateTime::from_timestamp_millis(t).unwrap())
                } else {
                    false
                })
            .collect();
        if let Ok(val) = self.executed_trades.filter(&mask) {
            Some(val)
        } else {
            None
        }
    }

    /// Select open positions that are less than price
    ///
    /// This is intended to be used to select open positions that are less than the current price
    /// and are therefore *may* be profitable.
    ///
    /// # Arguments
    /// * `price` - The price to compare against
    ///
    /// # Returns
    /// A dataframe containing open positions that are less than price
    /// If there are no open positions or no open positions that are less than price, `None` is returned.
    fn select_open_positions(&self, price: f64) -> Option<DataFrame> {
        if self.open_positions.is_empty() {
            return None;
        }

        // create a mask for all rows in `executed_trades` who's price is lte `price`
        let mask = self.executed_trades.column("price").unwrap()
            .f64().unwrap()
            .lt_eq(price);
        if let Ok(df) = self.executed_trades.filter(&mask) {
            if df.height() > 0 {
                return Some(df)
            }
        }
        None
    }
}


#[cfg(test)]
mod tests {
    use chrono::{Duration, NaiveDateTime, Utc};
    use crate::portfolio::{
        Portfolio,
        position::PositionHandlers,
        trade::TradeHandlers
    };
    use crate::types::{
        signals::Side,
        trades::executed::ExecutedTrade,
    };
    use crate::types::trades::Trade;

    /// Test that open positions are correctly added to the `open_positions` vector.
    /// Also ensure that closed positions are not added to the `open_positions` vector.
    #[test]
    fn test_set_as_open_position() {
        let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let mut portfolio = Portfolio::new(100.0, 100.0, None);

        assert!(portfolio.open_positions.is_empty());

        // add a buy and assert it is added to `open_positions`
        let trade = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            1.0,
            1.0,
            time,
        );
        portfolio.add_executed_trade(trade);

        portfolio.set_as_open_position();
        assert_eq!(portfolio.open_positions.len(), 1);

        // add a sell and assert it is *not* added to `open_positions`
        let trade = ExecutedTrade::new(
            "id".to_string(),
            Side::Sell,
            1.0,
            1.0,
            time + Duration::minutes(1),
        );
        portfolio.add_executed_trade(trade);

        portfolio.set_as_open_position();
        assert_eq!(portfolio.open_positions.len(), 1);

        // add another buy and assert it is added to `open_positions`
        let time2 = time + Duration::minutes(2);
        let trade = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            1.0,
            1.0,
            time2,
        );
        portfolio.add_executed_trade(trade);

        portfolio.set_as_open_position();
        assert_eq!(portfolio.open_positions.len(), 2);

        // ensure that the time values are correct in `open_positions`
        assert!(portfolio.open_positions.contains(&time));
        assert!(portfolio.open_positions.contains(&time2));
    }

    #[test]
    fn test_get_open_positions() {
        // create a portfolio with some executed trades
        // only `trade` and `trade3` should be returned by `get_open_positions`
        let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let trade = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            1.0,
            1.0,
            time
        );
        let trade2 = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            1.5,
            0.9,
            time + Duration::seconds(1)
        );
        let trade3 = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            1.7,
            1.5,
            time + Duration::seconds(2)
        );
        let trade4 = ExecutedTrade::new(
            "id".to_string(),
            Side::Sell,
            1.0,
            1.0,
            time + Duration::seconds(3)
        );

        let mut portfolio = Portfolio::new(100.0, 100.0, None);
        portfolio.add_executed_trade(trade);
        portfolio.add_executed_trade(trade2);
        portfolio.add_executed_trade(trade3);
        portfolio.add_executed_trade(trade4);

        assert_eq!(portfolio.get_open_positions(), None);

        // manually create open positions
        // add timestamps for trade and trade3 to `open_positions`
        portfolio.open_positions.push(time);
        portfolio.open_positions.push(time + Duration::seconds(2));

        // assert that the dataframe returned by `get_open_positions` corresponds to open trades
        assert_eq!(portfolio.get_open_positions().unwrap().height(), 2);

        let expected_quantity_sum = 1.0 + 1.5;
        let expected_price_sum = 1.0 + 1.7;

        let open_positions = portfolio.get_open_positions().unwrap();
        assert_eq!(open_positions.column("quantity").unwrap().sum::<f64>().unwrap(), expected_quantity_sum);
        assert_eq!(open_positions.column("price").unwrap().sum::<f64>().unwrap(), expected_price_sum);

        assert_eq!(open_positions.get_column_names(), &["id", "side", "price", "quantity", "cost", "point"]);
    }

    #[test]
    fn test_select_open_positions() {
        let time = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();
        let trade = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            2.0,
            1.0,
            time + Duration::seconds(1)
        );
        let trade2 = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            1.9,
            1.0,
            time + Duration::seconds(2)
        );
        let trade3 = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            1.8,
            1.0,
            time + Duration::seconds(3)
        );
        let trade4 = ExecutedTrade::new(
            "id".to_string(),
            Side::Buy,
            1.0,
            1.0,
            time + Duration::seconds(4)
        );

        let mut portfolio = Portfolio::new(100.0, 100.0, None);

        // manually create open positions, but do not set it yet
        let open_positions = vec![
            *trade.get_point(), *trade2.get_point(), *trade3.get_point(), *trade4.get_point()
        ];

        // add trades to `executed_trades`
        portfolio.add_executed_trade(trade);
        portfolio.add_executed_trade(trade2);
        portfolio.add_executed_trade(trade3);
        portfolio.add_executed_trade(trade4);


        // assert that `None` is returned when there are no open positions
        assert_eq!(portfolio.select_open_positions(1.0), None);

        // add open positions to `portfolio`
        portfolio.open_positions = open_positions;

        // assert that `None` is returned when price is 0.9
        let selected_open_positions = portfolio.select_open_positions(0.9);
        assert_eq!(selected_open_positions, None);

        // assert that the correct number of open positions are returned
        // we will be selecting trades that are less than 1.9
        // therefore only `trade2`, `trade3`, and `trade4` should be returned
        let selected_open_positions = portfolio.select_open_positions(1.9).unwrap();

        assert_eq!(selected_open_positions.height(), 3);
        assert_eq!(selected_open_positions.column("price").unwrap().sum::<f64>().unwrap(), 1.9 + 1.8 + 1.0);
        assert_eq!(selected_open_positions.column("quantity").unwrap().sum::<f64>().unwrap(), 1.0 * 3.0);
    }
}