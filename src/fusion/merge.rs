use super::compute;
use arrow_schema::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub async fn merge_batchrecord(
    schema: Arc<Schema>,
    records: Vec<RecordBatch>,
    sort_by: &[(&str, bool)],
) -> Result<RecordBatch, anyhow::Error> {
    let records = records
        .into_iter()
        .map(|batch| compute::cast(&schema, batch))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(compute::sort_batches(schema, records, sort_by).await?)
}
