use chrono::NaiveDateTime;
use polars::prelude::*;
use ta::indicators::{BollingerBands, BollingerBandsOutput};
use ta::Next;
use crate::indicators::{extract_new_rows, IndicatorUtilities, IndicatorGraphHandler};

const DEFAULT_PERIOD: usize = 20;
const DEFAULT_MULTIPLIER: f64 = 2.0;
const SOURCE_COL_NAME: &str = "close";

struct BBands {
    period: usize,
    multiplier: f64,

    indicator: BollingerBands,
    history: Option<DataFrame>,
    signals: Option<DataFrame>
}

impl BBands {
    pub fn new(period: usize, multiplier: f64) -> Self {
        Self {
            period,
            multiplier,
            history: None,
            indicator: BollingerBands::new(period, multiplier).unwrap(),
            signals: None
        }
    }
}
impl IndicatorUtilities for BBands {
    type Output = BollingerBandsOutput;

    fn restart_indicator(&mut self) {
        self.indicator = BollingerBands::new(self.period, self.multiplier).unwrap();
    }

    fn convert_output_to_dataframe(&self, output: Self::Output, timestamp: NaiveDateTime) -> DataFrame {
        let lower = output.lower;
        let middle = output.average;
        let upper = output.upper;

        let df = df!(
            "time" => &[timestamp],
            "lower" => &[lower],
            "middle" => &[middle],
            "upper" => &[upper],
        ).unwrap();

        df
    }

}

impl Default for BBands {
    fn default() -> Self {
        Self::new(DEFAULT_PERIOD, DEFAULT_MULTIPLIER)
    }
}

impl IndicatorGraphHandler for BBands {
    fn process_existing_candles(&mut self, candles: &DataFrame) {
        self.restart_indicator();

        let source = candles.column(SOURCE_COL_NAME).unwrap().f64().unwrap();
        let timestamps = candles.column("time").unwrap().datetime().unwrap();

        let output = source
            .into_iter()
            .zip(timestamps.into_iter())
            .map(|(value, timestamp)| {
                let output = self.indicator.next(value.unwrap());
                let timestamp = NaiveDateTime::from_timestamp_millis(timestamp.unwrap()).unwrap();
                self.convert_output_to_dataframe(output, timestamp)
            })
            .collect::<Vec<DataFrame>>();

        let mut df = output.get(0).unwrap().clone();
        for i in 1..output.len() {
            df = df.vstack(output.get(i).unwrap()).unwrap();
        }

        self.history = Some(df);
    }

    fn process_new_row(&mut self, candles: &DataFrame) {
        let row = extract_new_rows(
            candles,
            self.history.as_ref().unwrap()
        );

        // TODO: add the ability to handle all new rows
        assert_eq!(row.height(),
                   1,
                   "Row must be a single row.");
        assert_eq!(row.get_column_names(),
                   ["time", "open", "high", "low", "close", "volume"],
                   "Row has incorrect column names");

        // get the source column
        let data_point = row
            .column(SOURCE_COL_NAME).unwrap()
            .f64().unwrap().get(0).unwrap();

        // get the timestamp
        let timestamp = NaiveDateTime::from_timestamp_millis(row
            .column("time").unwrap()
            .datetime().unwrap().get(0).unwrap()
        ).unwrap();

        // update the indicator
        let output = self.indicator.next(data_point);

        // convert the output to a DataFrame
        let df = self.convert_output_to_dataframe(output, timestamp);

        // update the history
        if let Some(ref mut history) = self.history {
            *history = history.vstack(&df).unwrap();
        } else {
            self.history = Some(df);
        }
    }

    fn get_indicator_history(&self) -> &Option<DataFrame> {
        &self.history
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use ta::Period;
    use polars::prelude::*;

    use crate::indicators::bbands::{DEFAULT_PERIOD, DEFAULT_MULTIPLIER};
    use crate::indicators::{IndicatorGraphHandler, IndicatorUtilities};

    #[test]
    fn test_new() {
        let bb = super::BBands::new(15, 4.0);
        assert_eq!(bb.period, 15);
        assert_eq!(bb.multiplier, 4.0);
        assert_eq!(bb.history, None);
        assert_eq!(bb.signals, None);

        assert_eq!(bb.indicator.period(), 15);
        assert_eq!(bb.indicator.multiplier(), 4.0);
    }

    #[test]
    fn test_default() {
        let bb = super::BBands::default();
        assert_eq!(bb.period, 20);
        assert_eq!(bb.multiplier, 2.0);
        assert_eq!(bb.history, None);
        assert_eq!(bb.signals, None);

        assert_eq!(bb.indicator.period(), DEFAULT_PERIOD);
        assert_eq!(bb.indicator.multiplier(), DEFAULT_MULTIPLIER);
    }

    #[test]
    fn test_restart_indicator() {
        let mut bb = super::BBands::new(15, 4.0);
        bb.restart_indicator();

        assert_eq!(bb.indicator.period(), 15);
        assert_eq!(bb.indicator.multiplier(), 4.0);
    }


    #[test]
    fn test_process_existing_candles() {
        let mut bb = super::BBands::new(4, 2.0);

        // set the candles
        let time = Utc::now().naive_utc();
        let date_range = date_range(
            "time",
            time - chrono::Duration::minutes(5),
            time,
            Duration::parse("1m"),
            ClosedWindow::Left,
            TimeUnit::Milliseconds,
            None
        ).unwrap();
        let candles = df!(
            "time" => date_range.clone(),
            "open" => &[1, 2, 3, 4, 5],
            "high" => &[1, 2, 3, 4, 5],
            "low" => &[1, 2, 3, 4, 5],
            "close" => &[1.0, 2.0, 3.0, 4.0, 5.0],
            "volume" => &[1, 2, 3, 4, 5],
        ).unwrap();

        bb.process_existing_candles(&candles);

        let history = bb.history.as_ref().unwrap();

        assert_eq!(history.shape(), (5, 4));

        println!("{:?}", history);

        for i in 0..6 {
            assert_eq!(history.column("time").unwrap().datetime().unwrap().get(i),
                       date_range.get(i));
        }

        assert_eq!(history.column("lower").unwrap().f64().unwrap().get(0), Some(1.0));
        assert_eq!(history.column("lower").unwrap().f64().unwrap().get(1), Some(0.5));

        assert_eq!(history.column("middle").unwrap().f64().unwrap().get(0), Some(1.0));
        assert_eq!(history.column("middle").unwrap().f64().unwrap().get(1), Some(1.5));

        assert_eq!(history.column("upper").unwrap().f64().unwrap().get(0), Some(1.0));
        assert_eq!(history.column("upper").unwrap().f64().unwrap().get(1), Some(2.5));
    }

    #[test]
    fn test_process_new() {
        // create candles
        let time = Utc::now().naive_utc();
        let date_range = date_range(
            "time",
            time - chrono::Duration::minutes(5),
            time,
            Duration::parse("1m"),
            ClosedWindow::Left,
            TimeUnit::Milliseconds,
            None
        ).unwrap();
        let mut candles = df!(
            "time" => date_range,
            "open" => &[1, 2, 3, 4, 5],
            "high" => &[1, 2, 3, 4, 5],
            "low" => &[1, 2, 3, 4, 5],
            "close" => &[1.0, 2.0, 3.0, 4.0, 5.0],
            "volume" => &[1, 2, 3, 4, 5],
        ).unwrap();

        // create indicator and run `process_existing_candles()`
        let mut bb = super::BBands::new(4, 2.0);

        bb.process_existing_candles(&candles);

        // assert that the history aligns with candle dimensions
        assert_eq!(bb.history.as_ref().unwrap().height(), 5);

        // append a row to the candles the run `process_new_row()`
        let new_row = df!(
            "time" => &[time.clone()],
            "open" => &[6],
            "high" => &[6],
            "low" => &[6],
            "close" => &[6.0],
            "volume" => &[6],
        ).unwrap();
        let candles = candles.vstack(&new_row).unwrap();

        bb.process_new_row(&candles);

        // assert that `history` has been updated with new row
        let history = bb.history.as_ref().unwrap();

        assert_eq!(history.height(), 6);
        assert_eq!(history.column("time").unwrap().datetime().unwrap().get(5), Some(time.timestamp_millis()));
    }
}