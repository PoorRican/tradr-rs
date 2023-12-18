
/// Abstracts reasons for trades being reject or denied
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum ReasonCode {
    #[default]
    /// Unknown reason
    Unknown = 0,
    /// Trade would not be profitable
    NotProfitable = 1,
    /// Trade was rejected by market
    MarketRejection = 2,
    /// An error occurred while posting trade
    PostError = 3,
    /// An error occurred while parsing data returned from market
    ParseError = 4,
    /// Insufficient funds to complete trade
    InsufficientFunds = 5,
}