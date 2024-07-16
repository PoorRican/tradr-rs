# v0.2.0

- Create function for extracting candle data from db
- Improve performance of `BBands` by implementing `polars` window functions. Functions are now required to be passed a subset of candle data instead of new rows.
- Rename `Strategy.get_signals` to `Strategy.get_all_signals`
- Add `get_name` method to `Indicator` trait