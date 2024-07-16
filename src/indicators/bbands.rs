use std::cmp::Ordering;
use crate::indicators::{Indicator, IndicatorGraphHandler, IndicatorSignalHandler, IndicatorUtilities};
use crate::types::Signal;
use polars::prelude::*;
use ta::Next;
use crate::utils::extract_new_rows;

const DEFAULT_PERIOD: usize = 20;
const DEFAULT_MULTIPLIER: f64 = 2.0;
const DEFAULT_THRESHOLD: f64 = 0.8;
const SOURCE_COL_NAME: &str = "close";

fn calculate_bollinger_bands(df: &DataFrame, column_name: &str, periods: usize, num_std: f64) -> Result<DataFrame, PolarsError> {
    let mut window_options = RollingOptionsFixedWindow::default();
    window_options.min_periods = periods;
    window_options.window_size = periods;

    // Calculate the simple moving average (middle band)
    let sma = df.column(column_name)?
        .rolling_mean(RollingOptionsFixedWindow::default())?;

    // Calculate the standard deviation
    let std_dev = df.column(column_name)?
        .rolling_std(window_options)?;

    // Calculate upper and lower bands
    let upper_band = sma.clone() + (std_dev.clone() * num_std);
    let lower_band = sma.clone() - (&std_dev * num_std);

    let index = df.column("time").unwrap();

    Ok(
        df!{
            "time" => index,
            "lower" => lower_band.unwrap(),
            "middle" => sma,
            "upper" => upper_band.unwrap()
        }?
    )
}


pub struct BBands {
    period: usize,
    multiplier: f64,

    history: Option<DataFrame>,
    signals: Option<DataFrame>,
}


struct BollingerBandsOutputNew {
    pub lower: Option<f64>,
    pub middle: Option<f64>,
    pub upper: Option<f64>,
}

impl BBands {
    pub fn new(period: usize, multiplier: f64) -> Self {
        Self {
            period,
            multiplier,
            history: None,
            signals: None,
        }
    }
}
impl IndicatorUtilities for BBands {
    fn restart_indicator(&mut self) {
        self.history = None;
        self.signals = None;
    }

}

impl Default for BBands {
    fn default() -> Self {
        Self::new(DEFAULT_PERIOD, DEFAULT_MULTIPLIER)
    }
}

impl IndicatorGraphHandler for BBands {
    fn process_graph_for_existing(&mut self, candles: &DataFrame) {
        self.restart_indicator();

        let output = calculate_bollinger_bands(
            candles,
            SOURCE_COL_NAME,
            DEFAULT_PERIOD,
            DEFAULT_MULTIPLIER
        ).unwrap();

        self.history = Some(output);
    }

    fn process_graph_for_new_candles(&mut self, candles: &DataFrame) -> Result<(), ()> {
        // TODO: check that height is greater than window/period
        assert_ne!(candles.height(), 1, "Dataframe must contain more than one row.");

        // Ensure candles include new data
        let extracted = extract_new_rows(candles, self.history.as_ref().unwrap());
        assert_eq!(extracted.height(), 1, "Dataframe does not have new data.");

        // check validity of row
        assert_eq!(
            candles.get_column_names(),
            ["time", "open", "high", "low", "close", "volume"],
            "Row has incorrect column names"
        );

        // recalculate bollinger bands for a limited subset
        let last = candles
            .tail(Some(self.period));
        let output = calculate_bollinger_bands(
            &last,
            SOURCE_COL_NAME,
            self.period,
            self.multiplier
        ).unwrap();

        let new_row = output
            .tail(Some(1));

        // update the history
        if let Some(ref mut history) = self.history {
            *history = history.vstack(&new_row).unwrap();
        } else {
            self.history = Some(new_row);
        }

        Ok(())
    }

    fn get_indicator_history(&self) -> Option<&DataFrame> {
        self.history.as_ref()
    }
}

impl IndicatorSignalHandler for BBands {
    fn process_signals_for_existing(&mut self, candles: &DataFrame) {
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
                signal.unwrap_or(Signal::Hold).into()
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

    fn process_signals_for_new_candles(&mut self, candles: &DataFrame) -> Result<(), ()> {


       // TODO: this has to be updated!


        let graph_row = extract_new_rows(
            self.history.as_ref().unwrap(),
            self.signals.as_ref().unwrap(),
        );
        assert_eq!(graph_row.height(), 1, "Indicator graph is too ahead of signals. Call bootstrapping again.");

        let new_row = extract_new_rows(candles, self.signals.as_ref().unwrap());
        assert_eq!(new_row.height(), 1, "Passed dataframe might have duplicated timestamps.");

        assert_eq!(
            graph_row.column("time").unwrap().datetime().unwrap().get(0),
            new_row
                .column("time")
                .unwrap()
                .datetime()
                .unwrap()
                .get(0),
            "Graph row and new candle row must have the same timestamp"
        );

        let candle_price = new_row
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
                let signal = calculate_signal(series, candle_price, DEFAULT_THRESHOLD)
                    .unwrap_or(Signal::Hold);
                signal.into()
            })
            .collect::<Vec<i32>>();

        // update the signals
        let df = df!(
            "time" => new_row.column("time").unwrap(),
            "signal" => graph_row
        )
        .unwrap();

        if let Some(ref mut signals) = self.signals {
            *signals = signals.vstack(&df).unwrap();
        } else {
            self.signals = Some(df);
        }

        Ok(())
    }

    fn get_signal_history(&self) -> Option<&DataFrame> {
        self.signals.as_ref()
    }
}

impl Indicator for BBands {
    fn get_name(&self) -> &'static str {
        "bbands"
    }
}

#[derive(Debug)]
enum BBandExtractionError {
    InvalidSeriesLength,
    InvalidDataType,
    MissingValue,
}

/// Calculate signal from indicator and close price
///
/// This function uses a threshold to determine where the close price is relative to the bounds of the
/// Bollinger Bands.
///
/// # Arguments
/// * `series` - The indicator series.
/// * `candle_price` - The current candle price.
/// * `threshold` - The threshold to use when calculating the signal. This is expected to be a percentage.
///     The higher the value, the more closely the candle price must be to the bounds of the Bollinger Bands
///
/// # Returns
/// A `Signal` enum
fn calculate_signal(series: &[AnyValue], candle_price: f64, threshold: f64) -> Result<Signal, BBandExtractionError> {
    if series.len() != 4 {
        return Err(BBandExtractionError::InvalidSeriesLength);
    }

    let extract_float = |index: usize| -> Result<f64, BBandExtractionError> {
        match series.get(index) {
            Some(AnyValue::Float64(value)) => Ok(*value),
            Some(_) => Err(BBandExtractionError::InvalidDataType),
            None => Err(BBandExtractionError::MissingValue),
        }
    };

    let lower = extract_float(1)?;
    let middle = extract_float(2)?;
    let upper = extract_float(3)?;

    let buy_threshold = middle - (middle - lower) * threshold;
    let sell_threshold = middle + (upper - middle) * threshold;

    // TODO: add flag to hold if indicators are equal to thresholds
    Ok(match candle_price.partial_cmp(&buy_threshold) {
        Some(Ordering::Less) => Signal::Buy,
        Some(Ordering::Equal) => Signal::Buy,
        _ => match candle_price.partial_cmp(&sell_threshold) {
            Some(Ordering::Greater) => Signal::Sell,
            Some(Ordering::Equal) => Signal::Sell,
            _ => Signal::Hold,
        },
    })
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use polars::prelude::*;

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
    }

    #[test]
    fn test_default() {
        let bb = super::BBands::default();
        assert_eq!(bb.period, 20);
        assert_eq!(bb.multiplier, 2.0);
        assert_eq!(bb.history, None);
        assert_eq!(bb.signals, None);
    }

    #[test]
    fn test_restart_indicator() {
        let mut bb = super::BBands::new(15, 4.0);
        bb.history = Some(df!{
            "time" => &[Utc::now().naive_utc()],
            "lower" => &[1.0],
            "middle" => &[2.0],
            "upper" => &[3.0],
        }.unwrap());

        bb.signals = Some(df!{
            "time" => &[Utc::now().naive_utc()],
            "signal" => &[1],
        }.unwrap());

        bb.restart_indicator();

        assert!(bb.history.is_none());
        assert!(bb.signals.is_none());
    }

    #[test]
    fn test_process_existing_candles() {
        let mut bb = super::BBands::new(DEFAULT_PERIOD, DEFAULT_MULTIPLIER);

        // set the candles
        let time = Utc::now().naive_utc();
        let date_range = date_range(
            "time",
            time - chrono::Duration::minutes(25),
            time,
            Duration::parse("1m"),
            ClosedWindow::Left,
            TimeUnit::Milliseconds,
            None,
        )
        .unwrap();
        let candles = df!(
            "time" => date_range.clone(),
            "open" => &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25],
            "high" => &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25],
            "low" => &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25],
            "close" => &[
                1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0,
                14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0],
            "volume" => &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25],
        )
        .unwrap();

        bb.process_graph_for_existing(&candles);

        let history = bb.history.as_ref().unwrap();

        assert_eq!(history.shape(), (25, 4));

        // ensure that history has correct timestamp
        for i in 0..date_range.len() {
            assert_eq!(
                history.column("time").unwrap().datetime().unwrap().get(i),
                date_range.get(i)
            );
        };

        // ensure that upper and lower bounds have null values
        for i in 0..19 {
            assert_eq!(
                history.column("lower").unwrap().f64().unwrap().get(i),
                None
            );
            assert_eq!(
                history.column("upper").unwrap().f64().unwrap().get(i),
                None
            );
        }

        // ensure that upper/lower bounds have generally correct values
        // ensure that middle band is correct
        for i in 19..25 {
            // check for general ranges
            assert!(history.column("lower").unwrap().f64().unwrap().get(i).unwrap() < i as f64 - 2.0);
            assert!(history.column("upper").unwrap().f64().unwrap().get(i).unwrap() > i as f64 + 2.0);

            assert_eq!(
                history.column("middle").unwrap().f64().unwrap().get(i).unwrap(),
                i as f64
            );
        }
    }

    #[test]
    fn test_process_graph_for_new_candles() {
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

        bb.process_graph_for_existing(&candles);

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
        let new_data = candles.vstack(&new_row).unwrap();
        let result = bb.process_graph_for_new_candles(&new_data);

        assert!(result.is_ok());

        // assert that `history` has been updated with new row
        let history = bb.history.as_ref().unwrap();

        assert_eq!(history.height(), 6);
        assert_eq!(
            history.column("time").unwrap().datetime().unwrap().get(5),
            Some(time.and_utc().timestamp_millis())
        );
    }

    #[test]
    fn test_process_signals_for_existing() {
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

        bb.process_signals_for_existing(&candles);

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
    fn test_process_signals_signals_for_new_candles() {
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

        let old_candles = df!(
            "time" => date_range.clone(),
            "open" => &[1, 2, 3, 4, 5],
            "high" => &[1, 2, 3, 4, 5],
            "low" => &[1, 2, 3, 4, 5],
            "close" => &[0.9, 1.1, 1.3, 1.7, 1.9],
            "volume" => &[1, 2, 3, 4, 5])
            .unwrap();
        let new_row = df!(
            "time" => &[time],
            "open" => &[6],
            "high" => &[6],
            "low" => &[6],
            "close" => &[6.0],
            "volume" => &[6])
            .unwrap();

        let candles = old_candles.vstack(&new_row).unwrap();

        // call process_new_data() and assert that signals have been updated
        let result = bb.process_signals_for_new_candles(&candles);
        assert!(result.is_ok());

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
