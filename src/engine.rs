use chrono::NaiveDateTime;
use polars::frame::DataFrame;
use crate::markets::{BaseMarket};
use crate::markets::manager::CandleManager;
use crate::portfolio::{Portfolio, TradeHandlers};
use crate::strategies::Strategy;
use crate::types::{FutureTrade, Side};

pub struct Engine<T>
where T: BaseMarket {
    current_interval: String,
    trading_pair: String,

    portfolio: Portfolio,
    strategy: Strategy,
    market: T,
    manager: CandleManager<T>,
}

impl<T> Engine<T>
where T: BaseMarket {
    pub fn new(
        current_interval: &str,
        portfolio: Portfolio,
        strategy: Strategy,
        pair: &str,
        market: T,
    ) -> Self {
        let manager = CandleManager::new(pair, market.clone());
        let current_interval = current_interval.to_string();
        let trading_pair = pair.to_string();
        Self {
            current_interval,
            trading_pair,
            portfolio,
            strategy,
            market,
            manager,
        }
    }

    pub async fn bootstrap(&mut self) {
        self.manager.update_all().await;
        self.strategy.bootstrap(self.manager.get(&self.current_interval).unwrap().clone());
    }

    pub async fn run(&mut self) {
        let new_row = self.manager
            .update(&self.current_interval)
            .await
            .unwrap();
        assert_eq!(new_row.height(), 1);

        // pass row to strategy
        let signal = self.strategy.process(&new_row);

        let side = match Side::try_from(signal) {
            Ok(side) => side,
            Err(_) => return,
        };

        // generate rate
        let rate = match side {
            Side::Buy => generate_buy_rate(&new_row),
            Side::Sell => generate_sell_rate(&new_row),
        };

        // propose a trade
        let trade = match side {
            Side::Buy => {
                if self.portfolio.able_to_buy() {
                    let amount = self.portfolio.get_buy_amount();
                    let point = NaiveDateTime::from_timestamp_millis(
                        new_row.column("time")
                            .unwrap()
                            .datetime()
                            .unwrap()
                            .get(0)
                            .unwrap()).unwrap();
                    Some(FutureTrade::new(
                        side,
                        rate,
                        amount,
                        point
                    ))
                } else {
                    None
                }
            }
            Side::Sell => {
                self.portfolio.is_rate_profitable(rate)
            }
        };

        // if a trade has been proposed, submit it to the market
        if let Some(trade) = trade {
            let executed = self.market.submit_order(trade, self.trading_pair.clone())
                .await
                .unwrap();
            self.portfolio.add_executed_trade(executed);
        }
    }
}

fn generate_buy_rate(row: &DataFrame) -> f64 {
    let close = row.column("close").unwrap().f64().unwrap().get(0).unwrap();
    let high = row.column("high").unwrap().f64().unwrap().get(0).unwrap();
    let open = row.column("open").unwrap().f64().unwrap().get(0).unwrap();

    let sum = (close * 2.0) + high + open;
    sum / 4.0
}

fn generate_sell_rate(row: &DataFrame) -> f64 {
    let close = row.column("close").unwrap().f64().unwrap().get(0).unwrap();
    let low = row.column("low").unwrap().f64().unwrap().get(0).unwrap();
    let open = row.column("open").unwrap().f64().unwrap().get(0).unwrap();

    let sum = (close * 2.0) + low + open;
    sum / 4.0
}