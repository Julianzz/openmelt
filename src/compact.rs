use arrow_schema::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

// pub struct CompactService {
//     storage: Storage,
// }

// pub struct SegmentInfo {
//     path: String,
//     records: Vec<RecordBatch>,
// }

// merge schema
// tranlate data
// merge data
pub async fn merge_segments(
    schema: Arc<Schema>,
    records: Vec<RecordBatch>,
) -> Result<(), anyhow::Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow_schema::Schema;

    #[test]
    fn test_name() {
        let items = vec![Schema::empty()];
        // merge_schema(items.iter());
    }
}
