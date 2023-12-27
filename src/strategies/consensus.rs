use crate::strategies::Strategy;
use crate::types::{FutureTrade, Signal};

/// The [`Consensus`] enum is used to define how a [`Strategy`] should reach a consensus
/// between multiple [`Indicator`] objects.
pub enum Consensus {
    /// The [`Strategy`] will produce a [`FutureTrade`] if all [`Signal`]s returned by
    /// [`Indicator`] objects are the same.
    Unison,
    /// The [`Strategy`] will produce a [`FutureTrade`] if the majority of [`Signal`]s
    /// returned by [`Indicator`] objects are the same.
    Majority,
}

impl Consensus {
    /// Returns the consensus type as a string.
    fn as_str(&self) -> &str {
        match self {
            Consensus::Unison => "unison",
            Consensus::Majority => "majority",
        }
    }

    /// Accepts an iterator of [`Signal`]s and returns a [`Signal`] based on the consensus type
    ///
    /// # Arguments
    /// * `signals` - An iterator of [`Signal`]s
    ///
    /// # Returns
    /// A [`Signal`] based on the consensus type
    pub fn reduce(&self, signals: impl Iterator<Item = Signal>) -> Signal {
        let mut iter = signals.into_iter();
        match self {
            Consensus::Unison => {
                let first = iter.next().unwrap();
                if iter.all(|x| x == first) {
                    first
                } else {
                    Signal::Hold
                }
            },
            Consensus::Majority => {
                let mut buy = 0;
                let mut sell = 0;
                let mut hold = 0;
                for signal in iter {
                    match signal {
                        Signal::Buy => buy += 1,
                        Signal::Sell => sell += 1,
                        Signal::Hold => hold += 1,
                    }
                }
                if buy > sell && buy > hold {
                    Signal::Buy
                } else if sell > buy && sell > hold {
                    Signal::Sell
                } else {
                    Signal::Hold
                }
            },
        }
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn test_consensus_reduce_unison() {
        use super::*;
        let consensus = Consensus::Unison;

        // test when all signals are the same
        let signals = vec![Signal::Buy, Signal::Buy, Signal::Buy];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Buy);

        let signals = vec![Signal::Sell, Signal::Sell, Signal::Sell];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Sell);

        let signals = vec![Signal::Hold, Signal::Hold, Signal::Hold];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Hold);

        // test when all signals are different
        let signals = vec![Signal::Buy, Signal::Sell, Signal::Buy];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Hold);

        let signals = vec![Signal::Sell, Signal::Buy, Signal::Sell];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Hold);

        let signals = vec![Signal::Sell, Signal::Hold, Signal::Sell];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Hold);

        let signals = vec![Signal::Sell, Signal::Hold, Signal::Buy];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Hold);
    }

    #[test]
    fn test_consensus_reduce_majority() {
        use super::*;
        let consensus = Consensus::Majority;

        // test when all signals are the same
        let signals = vec![Signal::Buy, Signal::Buy, Signal::Buy];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Buy);

        let signals = vec![Signal::Sell, Signal::Sell, Signal::Sell];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Sell);

        let signals = vec![Signal::Hold, Signal::Hold, Signal::Hold];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Hold);

        // test when majority is buy
        let signals = vec![Signal::Buy, Signal::Buy, Signal::Sell];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Buy);

        let signals = vec![Signal::Buy, Signal::Buy, Signal::Hold];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Buy);

        // test when majority is sell
        let signals = vec![Signal::Sell, Signal::Sell, Signal::Buy];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Sell);

        let signals = vec![Signal::Sell, Signal::Sell, Signal::Hold];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Sell);

        // test when majority is hold
        let signals = vec![Signal::Hold, Signal::Hold, Signal::Buy];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Hold);

        let signals = vec![Signal::Hold, Signal::Hold, Signal::Sell];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Hold);

        // test ambiguous majority
        let signals = vec![Signal::Buy, Signal::Sell, Signal::Hold];
        assert_eq!(consensus.reduce(signals.into_iter()), Signal::Hold);
    }
}