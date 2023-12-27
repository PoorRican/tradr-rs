use crate::indicators::{extract_new_rows, Indicator, IndicatorGraphHandler, IndicatorSignalHandler, IndicatorUtilities};
use crate::types::Signal;
use chrono::NaiveDateTime;
use polars::prelude::*;
use ta::indicators::{BollingerBands, BollingerBandsOutput};
use ta::Next;

const DEFAULT_PERIOD: usize = 20;
const DEFAULT_MULTIPLIER: f64 = 2.0;
const DEFAULT_THRESHOLD: f64 = 0.8;
const SOURCE_COL_NAME: &str = "close";

struct BBands {
    period: usize,
    multiplier: f64,

    indicator: BollingerBands,
    history: Option<DataFrame>,
    signals: Option<DataFrame>,
}

impl BBands {
    pub fn new(period: usize, multiplier: f64) -> Self {
        Self {
            period,
            multiplier,
            history: None,
            indicator: BollingerBands::new(period, multiplier).unwrap(),
            signals: None,
        }
    }
}
impl IndicatorUtilities for BBands {
    fn restart_indicator(&mut self) {
        self.indicator = BollingerBands::new(self.period, self.multiplier).unwrap();
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

    fn process_new_candles(&mut self, row: &DataFrame) {
        // check for duplicated timestamps
        let duplicated = extract_new_rows(row, self.history.as_ref().unwrap());
        assert_eq!(duplicated.height(), 1, "Passed dataframe might have duplicated timestamps.");

        // check validity of row
        assert_eq!(row.height(), 1, "Row must be a single row.");
        assert_eq!(
            row.get_column_names(),
            ["time", "open", "high", "low", "close", "volume"],
            "Row has incorrect column names"
        );

        // get the candle price from source column
        let data_point = row
            .column(SOURCE_COL_NAME)
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();

        // get the timestamp
        let timestamp = NaiveDateTime::from_timestamp_millis(
            row.column("time")
                .unwrap()
                .datetime()
                .unwrap()
                .get(0)
                .unwrap(),
        )
        .unwrap();

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

impl IndicatorSignalHandler for BBands {
    fn process_existing_data(&mut self, candles: &DataFrame) {
        let candle_col = candles.column(SOURCE_COL_NAME).unwrap();

        let timestamps = candles.column("time").unwrap().clone();

        let signals = self
            .history
            .clone()
            .unwrap()
            .into_struct("rows")
            .into_iter()
            .zip(candle_col.iter())
            .map(|(series, close_price)| {
                let close_price = if let AnyValue::Float64(inner) = close_price {
                    inner
                } else {
                    panic!("Candle price must be a float")
                };

                let signal = calculate_signal(series, close_price, DEFAULT_THRESHOLD);
                signal.into()
            })
            .collect::<Vec<i32>>();

        self.signals = Some(
            df!(
                "time" => timestamps,
                "signal" => signals
            )
            .unwrap(),
        );
    }

    fn process_new_data(&mut self, row: &DataFrame) {
        let graph_row = extract_new_rows(
            self.history.as_ref().unwrap(),
            self.signals.as_ref().unwrap(),
        );

        let duplicated = extract_new_rows(row, self.signals.as_ref().unwrap());
        assert_eq!(duplicated.height(), 1, "Passed dataframe might have duplicated timestamps.");

        assert_eq!(graph_row.height(), 1, "Graph row must be a single row.");
        assert_eq!(row.height(), 1, "Candle row must be a single row.");
        assert_eq!(
            graph_row.column("time").unwrap().datetime().unwrap().get(0),
            row
                .column("time")
                .unwrap()
                .datetime()
                .unwrap()
                .get(0),
            "Graph row and candle row must have the same timestamp"
        );

        let candle_price = row
            .column(SOURCE_COL_NAME)
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();

        // process the graph row
        let graph_row = graph_row
            .into_struct("row")
            .into_iter()
            .map(|series| {
                let signal = calculate_signal(series, candle_price, DEFAULT_THRESHOLD);
                signal.into()
            })
            .collect::<Vec<i32>>();

        // update the signals
        let df = df!(
            "time" => row.column("time").unwrap(),
            "signal" => graph_row
        )
        .unwrap();

        if let Some(ref mut signals) = self.signals {
            *signals = signals.vstack(&df).unwrap();
        } else {
            self.signals = Some(df);
        }
    }

    fn get_signal_history(&self) -> &Option<DataFrame> {
        &self.signals
    }
}

impl Indicator for BBands {}

/// Convert indicator output to a DataFrame
///
/// This is used to convert the indicator output to a DataFrame with a single row
/// used to build the history DataFrame.
///
/// # Arguments
/// * `output` - The indicator output (`ta` specific)
/// * `timestamp` - The timestamp of the candle
///
/// # Returns
/// A DataFrame with a single row to use as a history row
fn convert_output_to_dataframe(
    output: BollingerBandsOutput,
    timestamp: NaiveDateTime,
) -> DataFrame {
    let lower = output.lower;
    let middle = output.average;
    let upper = output.upper;

    let df = df!(
            "time" => &[timestamp],
            "lower" => &[lower],
            "middle" => &[middle],
            "upper" => &[upper],
        )
        .unwrap();

    df
}

/// Unwrap a row from the indicator graph into a `BollingerBandsOutput`
///
/// The expected series index values are as follows:
/// * 0: the timestamp
/// * 1: the lower bound
/// * 2: the middle/average bound
/// * 3: the upper bound
///
/// # Arguments
/// * `series` - The row from the indicator graph.
///
/// # Returns
/// A `BollingerBandsOutput` struct
fn unwrap_output_series(series: &[AnyValue]) -> BollingerBandsOutput {
    assert_eq!(series.len(), 4, "Series must have 4 values");

    let lower = series.get(1).unwrap();
    let middle = series.get(2).unwrap();
    let upper = series.get(3).unwrap();

    let lower = if let AnyValue::Float64(inner) = lower {
        *inner
    } else {
        panic!("Could not get lower value from time-series chart")
    };

    let middle = if let AnyValue::Float64(inner) = middle {
        *inner
    } else {
        panic!("Could not get middle value from time-series chart")
    };

    let upper = if let AnyValue::Float64(inner) = upper {
        *inner
    } else {
        panic!("Could not get upper value from time-series chart")
    };

    BollingerBandsOutput {
        lower,
        average: middle,
        upper,
    }
}

/// Calculate signal from indicator and close price
///
/// This function uses a threshold to determine where the close price is relative to the bounds of the
/// Bollinger Bands.
///
/// # Arguments
/// * `series` - The indicator series. The values are fed to the `unwrap_output_series()` function
/// * `candle_price` - The current candle price.
/// * `threshold` - The threshold to use when calculating the signal. This is expected to be a percentage.
///     The higher the value, the more closely the candle price must be to the bounds of the Bollinger Bands
///
/// # Returns
/// A `Signal` enum
fn calculate_signal(series: &[AnyValue], candle_price: f64, threshold: f64) -> Signal {
    let output = unwrap_output_series(series);

    let buy_padding = (output.average - output.lower) * threshold;
    let buy_threshold = output.average - buy_padding;

    let sell_padding = (output.upper - output.average) * threshold;
    let sell_threshold = output.average + sell_padding;

    if candle_price <= buy_threshold {
        Signal::Buy
    } else if candle_price >= sell_threshold {
        Signal::Sell
    } else {
        Signal::Hold
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use polars::prelude::*;
    use ta::Period;

    use crate::indicators::bbands::{DEFAULT_MULTIPLIER, DEFAULT_PERIOD};
    use crate::indicators::{IndicatorGraphHandler, IndicatorSignalHandler, IndicatorUtilities};
    use crate::types::Signal;

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
            None,
        )
        .unwrap();
        let candles = df!(
            "time" => date_range.clone(),
            "open" => &[1, 2, 3, 4, 5],
            "high" => &[1, 2, 3, 4, 5],
            "low" => &[1, 2, 3, 4, 5],
            "close" => &[1.0, 2.0, 3.0, 4.0, 5.0],
            "volume" => &[1, 2, 3, 4, 5],
        )
        .unwrap();

        bb.process_existing_candles(&candles);

        let history = bb.history.as_ref().unwrap();

        assert_eq!(history.shape(), (5, 4));

        for i in 0..6 {
            assert_eq!(
                history.column("time").unwrap().datetime().unwrap().get(i),
                date_range.get(i)
            );
        }

        assert_eq!(
            history.column("lower").unwrap().f64().unwrap().get(0),
            Some(1.0)
        );
        assert_eq!(
            history.column("lower").unwrap().f64().unwrap().get(1),
            Some(0.5)
        );

        assert_eq!(
            history.column("middle").unwrap().f64().unwrap().get(0),
            Some(1.0)
        );
        assert_eq!(
            history.column("middle").unwrap().f64().unwrap().get(1),
            Some(1.5)
        );

        assert_eq!(
            history.column("upper").unwrap().f64().unwrap().get(0),
            Some(1.0)
        );
        assert_eq!(
            history.column("upper").unwrap().f64().unwrap().get(1),
            Some(2.5)
        );
    }

    #[test]
    fn test_process_new_candles() {
        // create candles
        let time = Utc::now().naive_utc();
        let date_range = date_range(
            "time",
            time - chrono::Duration::minutes(5),
            time,
            Duration::parse("1m"),
            ClosedWindow::Left,
            TimeUnit::Milliseconds,
            None,
        )
        .unwrap();
        let mut candles = df!(
            "time" => date_range,
            "open" => &[1, 2, 3, 4, 5],
            "high" => &[1, 2, 3, 4, 5],
            "low" => &[1, 2, 3, 4, 5],
            "close" => &[1.0, 2.0, 3.0, 4.0, 5.0],
            "volume" => &[1, 2, 3, 4, 5],
        )
        .unwrap();

        // create indicator and run `process_existing_candles()`
        let mut bb = super::BBands::new(4, 2.0);

        bb.process_existing_candles(&candles);

        // assert that the history aligns with candle dimensions
        assert_eq!(bb.history.as_ref().unwrap().height(), 5);

        // create a new candle row and run `process_new_row()`
        let new_row = df!(
            "time" => &[time.clone()],
            "open" => &[6],
            "high" => &[6],
            "low" => &[6],
            "close" => &[6.0],
            "volume" => &[6],
        )
        .unwrap();
        bb.process_new_candles(&new_row);

        // assert that `history` has been updated with new row
        let history = bb.history.as_ref().unwrap();

        assert_eq!(history.height(), 6);
        assert_eq!(
            history.column("time").unwrap().datetime().unwrap().get(5),
            Some(time.timestamp_millis())
        );
    }

    #[test]
    fn test_process_existing_data() {
        // create candles
        let time = Utc::now().naive_utc();
        let date_range = date_range(
            "time",
            time - chrono::Duration::minutes(6),
            time,
            Duration::parse("1m"),
            ClosedWindow::Left,
            TimeUnit::Milliseconds,
            None,
        )
        .unwrap();

        // candles and history should return the following signals:
        // buy: lower than lower bb
        // buy: lower than threshold bb but higher than lower bb
        // hold: higher than lower bb but lower than middle bb
        // hold: higher than middle bb but lower than upper bb
        // sell: higher than upper threshold but lower than upper bb
        // sell: higher than upper bb

        let candles = df!(
            "time" => date_range.clone(),
            "open" => &[1, 2, 3, 4, 5, 6],
            "high" => &[1, 2, 3, 4, 5, 6],
            "low" => &[1, 2, 3, 4, 5, 6],
            "close" => &[0.9, 1.1, 1.3, 1.7, 1.9, 2.1],
            "volume" => &[1, 2, 3, 4, 5, 6],
        )
        .unwrap();

        // create indicator history
        let history = df!(
            "time" => date_range,
            "lower" => &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            "middle" => &[1.5, 1.5, 1.5, 1.5, 1.5, 1.5],
            "upper" => &[2.0, 2.0, 2.0, 2.0, 2.0, 2.0],
        )
        .unwrap();

        let expected = &[
            Signal::Buy,
            Signal::Buy,
            Signal::Hold,
            Signal::Hold,
            Signal::Sell,
            Signal::Sell,
        ]
        .iter()
        .map(|signal| *signal as i32)
        .collect::<Vec<i32>>();

        let mut bb = super::BBands::new(4, 2.0);
        bb.history = Some(history);

        bb.process_existing_data(&candles);

        bb.signals
            .unwrap()
            .column("signal")
            .unwrap()
            .iter()
            .zip(expected.iter())
            .for_each(|(signal, expected)| {
                let signal = if let AnyValue::Int32(signal) = signal {
                    signal
                } else {
                    panic!("Could not get signal from time-series chart")
                };

                assert_eq!(signal, *expected);
            });
    }

    #[test]
    fn test_process_new_data() {
        let time = Utc::now().naive_utc();
        let date_range = date_range(
            "time",
            time - chrono::Duration::minutes(5),
            time,
            Duration::parse("1m"),
            ClosedWindow::Left,
            TimeUnit::Milliseconds,
            None,
        )
        .unwrap();

        // create history
        let history = df!(
            "time" => date_range.clone(),
            "lower" => &[1.0, 1.0, 1.0, 1.0, 1.0],
            "middle" => &[1.5, 1.5, 1.5, 1.5, 1.5],
            "upper" => &[2.0, 2.0, 2.0, 2.0, 2.0],
        )
        .unwrap();

        // create signals
        let signals = df!(
            "time" => date_range.clone(),
            "signal" => &[1, 1, 0, 0, -1],
        )
        .unwrap();

        // create indicator
        let mut bb = super::BBands::new(4, 2.0);
        bb.history = Some(history);
        bb.signals = Some(signals);

        assert_eq!(bb.history.as_ref().unwrap().height(), 5);
        assert_eq!(bb.signals.as_ref().unwrap().height(), 5);

        // update history with new row
        let new_history_row = df!(
            "time" => &[time.clone()],
            "lower" => &[1.0],
            "middle" => &[1.5],
            "upper" => &[2.0],
        )
        .unwrap();
        let history = bb.history.as_ref().unwrap().vstack(&new_history_row).unwrap();
        bb.history = Some(history);

        let new_row = df!(
            "time" => &[time],
            "open" => &[6],
            "high" => &[6],
            "low" => &[6],
            "close" => &[6.0],
            "volume" => &[6],
        )
        .unwrap();

        // call process_new_data() and assert that signals have been updated
        bb.process_new_data(&new_row);

        assert_eq!(bb.signals.as_ref().unwrap().height(), 6);
        assert_eq!(
            bb.signals
                .as_ref()
                .unwrap()
                .column("time")
                .unwrap()
                .datetime()
                .unwrap()
                .get(5)
                .unwrap(),
            time.timestamp_millis()
        );
        assert_eq!(
            bb.signals
                .as_ref()
                .unwrap()
                .column("signal")
                .unwrap()
                .i32()
                .unwrap()
                .get(5)
                .unwrap(),
            Signal::Sell as i32
        );
    }
}
