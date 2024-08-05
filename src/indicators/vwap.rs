use polars::prelude::*;
use crate::indicators::{GraphProcessingError, Indicator, IndicatorGraphHandler, IndicatorProcessingError, IndicatorSignalHandler, IndicatorUtilities, SignalExtractionError, SignalProcessingError};
use crate::processor::CandleProcessor;

/// The Volume Weighted Average Price (VWAP) indicator
///
/// Note that this will _NOT_ work with the way that `Indicator` is structured.
pub struct VWAP {
    /// The window size for the VWAP calculation
    window: usize,

    graph: Option<DataFrame>,
    signals: Option<DataFrame>,
}

impl VWAP {
    pub fn new(window: usize) -> Self {
        Self {
            window,
            graph: None,
            signals: None
        }
    }
}

impl IndicatorUtilities for VWAP {
    fn restart_indicator(&mut self) {
        self.graph = None;
        self.signals = None;
    }
}

impl IndicatorGraphHandler for VWAP {
    fn process_graph(&mut self, candles: &DataFrame) -> Result<(), GraphProcessingError> {
        let graph = calculate_vwap(candles, self.window)
            .map_err(|e| GraphProcessingError::DataFrameError(e))?;

        self.graph = Some(graph);

        Ok(())
    }

    fn process_graph_for_new_candles(&mut self, candles: &DataFrame) -> Result<(), GraphProcessingError> {
        todo!()
    }

    fn get_indicator_history(&self) -> Option<&DataFrame> {
        self.graph.as_ref()
    }
}

impl IndicatorSignalHandler for VWAP {
    fn process_signals(&mut self, candles: &DataFrame) -> Result<(), SignalProcessingError> {
        // TODO!
        Ok(())
    }

    fn process_signals_for_new_candles(&mut self, candles: &DataFrame) -> Result<(), SignalProcessingError> {
        todo!()
    }

    fn get_signal_history(&self) -> Option<&DataFrame> {
        todo!()
    }

    fn extract_signals(&self, graph: &DataFrame, candles: &DataFrame) -> Result<DataFrame, SignalExtractionError> {
        todo!()
    }
}

impl Indicator for VWAP {
    fn get_name(&self) -> &'static str {
        "vwap"
    }

    fn save_graph_as_csv(&mut self, path: &str) -> Result<(), PolarsError> {
        // TODO: raise error if graph is None
        if let Some(graph) = self.graph.as_mut() {
            let mut file = std::fs::File::create(path).unwrap();
            CsvWriter::new(&mut file).finish(graph)?;
        }
        Ok(())
    }
}

impl CandleProcessor for VWAP {
    type ReturnType = ();
    type ErrorType = IndicatorProcessingError;

    fn process_candles(&mut self, candles: &DataFrame) -> Result<(), Self::ErrorType> {
        self.process_graph(candles).map_err(|e| IndicatorProcessingError::GraphError(e))
    }
}

fn calculate_vwap(candles: &DataFrame, window: usize) -> Result<DataFrame, PolarsError> {
    // Calculate typical price: (high + low + close) / 3
    let df = candles
        .tail(Some(window))
        .lazy()
        .select([
            col("time"),
            col("volume"),
            ((col("high")
                + col("low")
                + col("close")
            ) / lit(3))
                .alias("typical_price")
        ]).collect()?;

    // Calculate cumulative typical price * volume and cumulative volume
    let df = df
        .lazy()
        .select([
            col("time"),
            col("volume").cum_sum(false).alias("cum_volume"),
            (col("typical_price") * col("volume")).cum_sum(false).alias("cum_tp_vol")
        ]).collect()?;

    println!("{:?}", df);

    // Calculate VWAP
    df.lazy()
        .select([
            col("time"),
            (col("cum_tp_vol") / col("cum_volume")).alias("vwap")
        ]).collect()
}