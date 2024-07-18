# CHANGELOG

## v0.3.0

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