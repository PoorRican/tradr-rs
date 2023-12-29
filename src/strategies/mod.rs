mod consensus;

use polars::frame::DataFrame;
use crate::types::Signal;
use crate::indicators::Indicator;
pub use crate::strategies::consensus::Consensus;


/// A [`IndicatorContainer`] is a collection of [`Indicator`] objects.
type IndicatorContainer = Vec<Box<dyn Indicator>>;

/// A [`Strategy`] is a facade for interfacing with more than one [`Indicator`] objects.
///
/// A simple interface is provided for bootstrapping historical candle data, processing new candle data,
/// and generating a consensus [`Signal`] among all [`Indicator`] objects.
pub struct Strategy {
    indicators: IndicatorContainer,
    consensus: Consensus,
}
impl Strategy {
    pub fn new(
        indicators: IndicatorContainer,
        consensus: Consensus
    ) -> Self {
        Self {
            indicators,
            consensus,
        }
    }

    /// Bootstrap historical candle data
    pub fn bootstrap(&mut self, data: DataFrame) {
        for indicator in self.indicators.iter_mut() {
            indicator.process_existing(&data);
        }
    }

    /// Process a new candle and generate a consensus [`Signal`] among the [`Indicator`] objects.
    ///
    /// Internally, the dataframe is propagated to all internal indicators, and the resulting
    /// signals are gathered. A consensus is then reached between the signals, and returned.
    ///
    /// # Arguments
    /// * `row` - The new candle data to process
    ///
    /// # Returns
    /// A [`Signal`] representing the consensus between all [`Indicator`] objects
    pub fn process(&mut self, row: DataFrame) -> Signal {
        for indicator in self.indicators.iter_mut() {
            indicator.process_new(&row);
        }

        let signals = self.indicators
            .iter()
            .map(|x| x.get_last_signal().expect("No signal found"))
            .collect::<Vec<Signal>>();

        self.consensus.reduce(signals.into_iter())
    }
}
