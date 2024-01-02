use std::sync::Arc;

use arrow_json::{writer::record_batches_to_json_rows, ReaderBuilder};
use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::{self, record_batch::RecordBatch};
use serde_json::map::Map as JsonMap;
use serde_json::Value;

pub fn json_to_recordbatch(
    schema: &Schema,
    record: &[Value],
) -> Result<RecordBatch, anyhow::Error> {
    let batch_size = arrow::util::bit_util::round_upto_multiple_of_64(record.len());
    let mut decoder = ReaderBuilder::new(Arc::new(schema.clone()))
        .with_batch_size(batch_size)
        .build_decoder()
        .unwrap();
    decoder.serialize(record)?;
    Ok(decoder.flush()?.unwrap())
}

pub fn recordbatch_to_jsons(
    batchs: &[&RecordBatch],
) -> Result<Vec<JsonMap<String, Value>>, anyhow::Error> {
    Ok(record_batches_to_json_rows(batchs)?)
}

#[allow(dead_code)]
pub fn build_tests_recordbatch() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int64, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int64Array::from(vec![1, 10, 10, 100])),
        ],
    )
    .unwrap();
    (schema, batch)
}
