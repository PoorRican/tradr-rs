# v0.2.0

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
