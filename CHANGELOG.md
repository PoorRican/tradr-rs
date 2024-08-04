# CHANGELOG

## v0.4.0

Massively change the API for setting up backtesting. Now, there is an object `MarketData` which is able to read all candles
for a given asset class from the candle database. `BacktestingRuntime` now accepts the name for the traded asset, the name
for the market data asset, and the intraday frequency.

### Code Changes

- Simplify `BacktestingRuntime::run`
- Rename `BacktestingRunner` to `BacktestingRuntime`

### New Structures

- Create a type `MarketData`

---

## v0.3.2

### Code Changes

- Add `data` dir to `.gitignore`
- Create some utils to clean and better organize `main.rs` and `backtesting.rs`
- Read backtesting config from a TOML file
- Add documentation to `PositionManagerConfig` fields

---

## v0.3.1

### Code Changes

- Show date ranges upon backtesting
- Show portfolio at the end of backtesting
- Automatically export candles and graph data to CSV
- Rename `bootstrap` functions to `process_historical_candles`
- Return `Result` from `Strategy.process_new_candles` and `Strategy.process_historical_candles`
- Create `CandleProcessor` trait as an interface for processing candles. Implement for `Indicator` objects and `Strategy`.

---

## v0.3.0

### Code Changes

- Remove `Engine`. This will be replaced with an updated implementation in the future.
- Remove `AsDataFrame` trait-bound from `Trade`. Removed implementations from `ExecutedTrade`, `FutureTrade`, and `Trade`.
- Implement `Decimal` as the floating-point type for `Trade`, `Candle`, and `Portfolio`.
- Move risk assessment functionality from `Portfolio` to a dedicated module. It is now a standalone function.
  Added documentation to the `PortfolioRisk` struct to explain the complex metrics.
- Created a `PositionManager` for handling trade decisions.
- Added `thiserror` and `log` crates
- Updated `Trade` and `ExecutedTrade` to use better nomenclature for trade types.
- Change default threshold for `BBands`
- Add logging for when outside of risk tolerance
- Change `PositionManager` defaults in main

### Portfolio Improvements

- Remove all internal of `DataFrame`. Instead, `Vec` and `HashMap` are used to store data.
- Add risk functions to `Portfolio`
- Improve open position tracking. Create an `OpenPosition` struct to track open positions,
  and add attributes to `Portfolio` to track open position metrics.

---

## v0.2.1

### New Features

- Implements a simple backtesting engine
- Use `rust_decimal` crate for accurate decimal arithmetic. This has not fully been implemented yet.

### Strategy Improvements

- Implement `Strategy.get_combined_signals` (untested)

### Portfolio Improvements

- Add performance metrics to `Portfolio`
- Add functions which propose `FutureTrades` - `generate_buy_opt()` and `generate_sell_opt()`
- Mark `Portfolio.is_rate_profitable` as deprecated. Usage must be removed from `Engine`.
- Add `PortfolioArgs` for `Portfolio` initialization
- Rename `get_capital` to `available_capital` for better readability
- Remove `Persistence` module. This will be replaced with a database implementation in the future.

### Indicator Improvements

- Add `Err` return values to `BBands.process_graph_for_new_candles()`
- Formally test `BBands.Gprocess_graph_for_new_candles()`

### Utility Improvements

- Create util for extracting `Vec<Candle>` from a dataframe
- Create util for extracting `Vec<Side>` from a dataframe

---

## v0.2.0

This release adds significant performance to `BBands`, and changes the API for `Strategy` and `Indicator` in preparation for a backtesting implementation.

- Create function for extracting candle data from db
- Improve performance of `BBands` by implementing `polars` window functions. Functions are now required to be passed a subset of candle data instead of new rows.
- Rename `Strategy.get_signals` to `Strategy.get_all_signals`
- Add `get_name` method to `Indicator` trait
- Convert `BBands.signals` to use `i8` instead of `i32` for signal type
- Change `Strategy.get_all_signals` to return a `DataFrame` instead of a `Vec<DataFrame>`
- In `BBands`, alter `calculate_signals` to accept 2 `DataFrame` objects
- Create a function `Strategy.get_all_graphs`
- Rename `BBands.history` to `BBands.graph`
- Rename `IndicatorGraphHandler.process_graph_for_existing` to `process_graph`
- Rename `IndicatorSignalHandler.process_signals_for_existing` to `process_signals`
- Create errors for signal / graph processing
- Add parameters to `BBands`
- Refactor `calculate_bollinger_bands` as a private method of `BBands`
- Refactor `calculate_signals` as a trait method of `IndicatorSignalHandlers`. Rename to `extract_signals`.