use polars::prelude::*;

pub fn save_candles(candles: &mut DataFrame, path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = std::fs::File::create(path).unwrap();
    CsvWriter::new(&mut file).finish(candles)?;

    Ok(())
}
