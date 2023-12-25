use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::{
    array::{Int64Array, StringArray},
    record_batch::RecordBatch,
};

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
