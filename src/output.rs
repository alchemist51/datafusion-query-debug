use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::print_batches;

pub fn output_json(results: &[RecordBatch]) -> Result<()> {
    output_table(results)
}

pub fn output_csv(results: &[RecordBatch]) -> Result<()> {
    output_table(results)
}

pub fn output_table(results: &[RecordBatch]) -> Result<()> {
    print_batches(results)?;
    Ok(())
}
