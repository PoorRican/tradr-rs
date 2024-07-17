use polars::prelude::*;
use crate::indicators::{GraphProcessingError, Indicator, IndicatorGraphHandler, IndicatorSignalHandler, IndicatorUtilities, SignalExtractionError, SignalProcessingError};
use crate::types::Signal;
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

    graph: Option<DataFrame>,
    signals: Option<DataFrame>,
}

impl BBands {
    pub fn new(period: usize, multiplier: f64) -> Self {
        Self {
            period,
            multiplier,
            graph: None,
            signals: None,
        }
    }
}
impl IndicatorUtilities for BBands {
    fn restart_indicator(&mut self) {
        self.graph = None;
        self.signals = None;
    }

}

impl Default for BBands {
    fn default() -> Self {
        Self::new(DEFAULT_PERIOD, DEFAULT_MULTIPLIER)
    }
}

impl IndicatorGraphHandler for BBands {
    fn process_graph(&mut self, candles: &DataFrame) -> Result<(), GraphProcessingError> {
        self.restart_indicator();

        match calculate_bollinger_bands(
            candles,
            SOURCE_COL_NAME,
            DEFAULT_PERIOD,
            DEFAULT_MULTIPLIER
        ) {
            Ok(output) => {
                self.graph = Some(output);
                Ok(())
            },
            Err(e) => Err(GraphProcessingError::DataFrameError(e))
        }
    }

    fn process_graph_for_new_candles(&mut self, candles: &DataFrame) -> Result<(), GraphProcessingError> {
        // TODO: check that height is greater than window/period
        assert_ne!(candles.height(), 1, "Dataframe must contain more than one row.");

        // Ensure candles include new data
        let extracted = extract_new_rows(candles, self.graph.as_ref().unwrap());
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
        if let Some(ref mut history) = self.graph {
            *history = history.vstack(&new_row).unwrap();
        } else {
            self.graph = Some(new_row);
        }

        Ok(())
    }

    fn get_indicator_history(&self) -> Option<&DataFrame> {
        self.graph.as_ref()
    }
}

impl IndicatorSignalHandler for BBands {
    fn process_signals(&mut self, candles: &DataFrame) -> Result<(), SignalProcessingError> {
        // ensure that the graph history exists
        return match &self.graph {
            None => {
                return Err(SignalProcessingError::GraphHistoryMissing)
            },
            Some(history) => {
                // ensure graph history is aligned with candles
                let history_aligned = extract_new_rows(candles, history);
                if history_aligned.height() != 0 {
                    return Err(SignalProcessingError::GraphHistoryBehindCandles)
                }

                // ensure that history and candles are the same number of rows
                if history.shape().0 != candles.shape().0 {
                    return Err(SignalProcessingError::GraphIndexNotAlignedWithCandles)
                }

                match calculate_signal(history, candles, DEFAULT_THRESHOLD) {
                    Ok(signals) => {
                        self.signals = Some(signals);
                        Ok(())
                    },
                    Err(e) => {
                        Err(SignalProcessingError::ExtractionError(e))
                    }
                }
            }
        }
    }

    fn process_signals_for_new_candles(&mut self, candles: &DataFrame) -> Result<(), SignalProcessingError> {
        let new_graph_rows = extract_new_rows(
            self.graph.as_ref().unwrap(),
            self.signals.as_ref().unwrap(),
        );

        let new_candle_rows = extract_new_rows(candles, self.signals.as_ref().unwrap());
        if new_candle_rows.height() == 0 {
            return Ok(());
        } else if new_candle_rows.height() != 1 {
            return Err(SignalProcessingError::DuplicatedCandleTimestamps);
        }

        let graph_start_index =
            new_graph_rows
                .column("time")
                .unwrap()
                .datetime()
                .unwrap()
                .get(0)
                .unwrap();
        let candle_start_index =
            new_candle_rows
                .column("time")
                .unwrap()
                .datetime()
                .unwrap()
                .get(0)
                .unwrap();
        if graph_start_index != candle_start_index {
            return Err(SignalProcessingError::GraphIndexNotAlignedWithCandles);
        }

        let new_signals = calculate_signal(
            &new_graph_rows, &new_candle_rows, DEFAULT_THRESHOLD
        ).unwrap();

        if let Some(ref mut signals) = self.signals {
            *signals = signals.vstack(&new_signals).unwrap();
        } else {
            self.signals = Some(new_signals);
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


/// Calculate signal from indicator and close price
///
/// This function uses a threshold to determine where the close price is relative to the bounds of the
/// Bollinger Bands.
///
/// # Arguments
/// * `graph` - The indicator graph
/// * `candles` - Candles
/// * `threshold` - The threshold to use when calculating the signal. This is expected to be a percentage.
///     The higher the value, the more closely the candle price must be to the bounds of the Bollinger Bands
///
/// # Returns
/// A `Signal` enum
fn calculate_signal(graph: &DataFrame, candles: &DataFrame, threshold: f64) -> Result<DataFrame, SignalExtractionError> {
    if graph.shape().1 != 4 {
        return Err(SignalExtractionError::InvalidGraphColumns);
    }

    let lower = graph.column("lower").unwrap().f64().unwrap();
    let middle = graph.column("middle").unwrap().f64().unwrap();
    let upper = graph.column("upper").unwrap().f64().unwrap();

    let candle_price = candles.column(SOURCE_COL_NAME).unwrap().f64().unwrap().clone();

    let buy_threshold = middle.clone() - (middle.clone() - lower.clone()) * threshold;
    let sell_threshold = middle.clone() + (upper.clone() - middle.clone()) * threshold;

    let index = candles.column("time").unwrap().clone();

    // put all the data into a dataframe
    let df = df![
        "time" => index.clone(),
        "buy_thresholds" => buy_threshold.into_series(),
        "sell_thresholds" => sell_threshold.into_series(),
        "candle_price" => candle_price.into_series()
    ].unwrap();

    // find where the thresholds are exceeded
    let threshold_exceeded = df.lazy().select([
        col("time"),
        col("candle_price").lt_eq(col("buy_thresholds")).alias("buy_signals"),
        col("candle_price").gt_eq(col("sell_thresholds")).alias("sell_signals"),
    ]).collect().unwrap();

    // combine the buy and sell signals into a single, numerical column
    let signals = threshold_exceeded.lazy()
        .with_column(when(col("sell_signals").eq(lit(true)))
            .then(Signal::Sell as i8)
            .otherwise(col("buy_signals").cast(DataType::Int8)).alias("trade_signals"))
        .collect().unwrap();

    // select only the time and trade_signals columns and cast the trade_signals column to an i8
    let signals = signals.lazy()
        .select([
            col("time"),
            col("trade_signals").cast(DataType::Int8)
        ]).collect().unwrap();

    // replace all null values with 0
    let signals = signals.lazy()
        .with_column(when(col("trade_signals").is_null())
            .then(Signal::Hold as i8)
            .otherwise(col("trade_signals")).alias("signals"));

    // select only the time and signals columns
    let signals = signals.lazy()
        .select([
            col("time"),
            col("signals")
        ]).collect().unwrap();

    Ok(signals)
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
        assert_eq!(bb.graph, None);
        assert_eq!(bb.signals, None);
    }

    #[test]
    fn test_default() {
        let bb = super::BBands::default();
        assert_eq!(bb.period, 20);
        assert_eq!(bb.multiplier, 2.0);
        assert_eq!(bb.graph, None);
        assert_eq!(bb.signals, None);
    }

    #[test]
    fn test_restart_indicator() {
        let mut bb = super::BBands::new(15, 4.0);
        bb.graph = Some(df!{
            "time" => &[Utc::now().naive_utc()],
            "lower" => &[1.0],
            "middle" => &[2.0],
            "upper" => &[3.0],
        }.unwrap());

        bb.signals = Some(df!{
            "time" => &[Utc::now().naive_utc()],
            "signals" => &[1],
        }.unwrap());

        bb.restart_indicator();

        assert!(bb.graph.is_none());
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

        let _ = bb.process_graph(&candles).unwrap_or_else(
            |e| panic!("Could not process graph: {:?}", e)
        );

        let history = bb.graph.as_ref().unwrap();

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

        bb.process_graph(&candles).unwrap_or_else(|e| panic!("Could not process graph: {:?}", e));

        // assert that the history aligns with candle dimensions
        assert_eq!(bb.graph.as_ref().unwrap().height(), 5);

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
        let history = bb.graph.as_ref().unwrap();

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
        .map(|signal| *signal as i8)
        .collect::<Vec<i8>>();

        let mut bb = super::BBands::new(4, 2.0);
        bb.graph = Some(history);

        bb.process_signals(&candles).unwrap_or_else(|e| panic!("Could not process signals: {:?}", e));

        bb.signals
            .unwrap()
            .column("signals")
            .unwrap()
            .iter()
            .zip(expected.iter())
            .for_each(|(signal, expected)| {
                let signal = if let AnyValue::Int8(signal) = signal {
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
            "signals" => &[1i8, 1i8, 0i8, 0i8, -1i8],
        )
        .unwrap();

        // create indicator
        let mut bb = super::BBands::new(4, 2.0);
        bb.graph = Some(history);
        bb.signals = Some(signals);

        assert_eq!(bb.graph.as_ref().unwrap().height(), 5);
        assert_eq!(bb.signals.as_ref().unwrap().height(), 5);

        // update history with new row
        let new_history_row = df!(
            "time" => &[time.clone()],
            "lower" => &[1.0],
            "middle" => &[1.5],
            "upper" => &[2.0],
        )
        .unwrap();
        let history = bb.graph.as_ref().unwrap().vstack(&new_history_row).unwrap();
        bb.graph = Some(history);

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
                .column("signals")
                .unwrap()
                .i8()
                .unwrap()
                .get(5)
                .unwrap(),
            Signal::Sell as i8
        );
    }
}
