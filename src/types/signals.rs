use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const BUY: isize = 1;
const SELL: isize = -1;

#[derive(Debug, Clone, Copy, PartialEq)]
/// Abstracts indicator outputs
pub enum Signal {
    Sell = SELL,
    Hold = 0,
    Buy = BUY,
}

impl Into<i32> for Signal {
    fn into(self) -> i32 {
        match self {
            Signal::Buy => BUY as i32,
            Signal::Hold => 0,
            Signal::Sell => SELL as i32,
        }
    }
}

impl From<i32> for Signal {
    fn from(value: i32) -> Self {
        match value as isize {
            SELL => Signal::Sell,
            0 => Signal::Hold,
            BUY => Signal::Buy,
            _ => panic!("Invalid signal value: {}", value),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
/// Abstracts types of trades
pub enum Side {
    Sell = SELL,
    Buy = BUY,
}

impl TryFrom<Signal> for Side {
    type Error = &'static str;

    fn try_from(value: Signal) -> Result<Self, Self::Error> {
        match value {
            Signal::Buy => Ok(Side::Buy),
            Signal::Sell => Ok(Side::Sell),
            Signal::Hold => Err("Cannot convert Signal::Hold to Side"),
        }
    }
}

impl Serialize for Side {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Side::Buy => serializer.serialize_str("buy"),
            Side::Sell => serializer.serialize_str("sell"),
        }
    }
}

impl<'de> Deserialize<'de> for Side {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "buy" => Ok(Side::Buy),
            "sell" => Ok(Side::Sell),
            _ => Err(Error::custom("Unexpected value for Side")),
        }
    }
}

impl From<i32> for Side {
    fn from(value: i32) -> Self {
        match value as isize {
            SELL => Side::Sell,
            BUY => Side::Buy,
            _ => panic!("Invalid side value: {}", value),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_side_from_i32() {
        assert_eq!(Side::from(1), Side::Buy);
        assert_eq!(Side::from(-1), Side::Sell);
    }

    #[test]
    #[should_panic]
    fn test_side_from_i32_panic() {
        let _ = Side::from(0);
    }

    #[test]
    fn test_side_serialize() {
        assert_eq!(serde_json::to_string(&Side::Buy).unwrap(), "\"buy\"");
        assert_eq!(serde_json::to_string(&Side::Sell).unwrap(), "\"sell\"");
    }

    #[test]
    fn test_side_deserialize() {
        assert_eq!(serde_json::from_str::<Side>("\"buy\"").unwrap(), Side::Buy);
        assert_eq!(
            serde_json::from_str::<Side>("\"sell\"").unwrap(),
            Side::Sell
        );
    }
}
