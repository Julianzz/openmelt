use std::sync::Arc;

use datafusion::arrow::{datatypes::Schema, record_batch::RecordBatch};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

// TODO to optimize
pub fn new_parquet_writer<'a>(
    dest: &'a mut Vec<u8>,
    schema: Arc<Schema>,
) -> Result<ArrowWriter<&'a mut Vec<u8>>, anyhow::Error> {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(false)
        .build();
    Ok(ArrowWriter::try_new(dest, schema, Some(props))?)
}

pub fn write_recordbatch(
    schema: Arc<Schema>,
    batch: &RecordBatch,
) -> Result<Vec<u8>, anyhow::Error> {
    let mut datas = Vec::new();
    // let batch = recordbatch::json_to_recordbatch(schema, &records)?;
    let mut writer = new_parquet_writer(&mut datas, schema)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(datas)
}

// use std::sync::Arc;

// use datafusion::arrow::datatypes::Schema;
// use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

// pub const PARQUET_BATCH_SIZE: usize = 8 * 1024;
// pub const PARQUET_PAGE_SIZE: usize = 1024 * 1024;
// pub const PARQUET_MAX_ROW_GROUP_SIZE: usize = 1024 * 1024;

// pub struct FileMeta {}

// pub fn new_parquet_writer<'a>(
//     buf: &'a mut Vec<u8>,
//     schema: &'a Arc<Schema>,
//     // bloom_filter: &'a [String],
//     // meta: &'a FileMeta,
// ) -> ArrowWriter<&'a mut Vec<u8>> {
//     let writer_props = WriterProperties::builder()
//         .set_write_batch_size(PARQUET_BATCH_SIZE)
//         .set_data_page_size_limit(PARQUET_PAGE_SIZE)
//         .set_max_row_group_size(PARQUET_MAX_ROW_GROUP_SIZE);

//     let writer_props = writer_props.build();
//     ArrowWriter::try_new(buf, schema.clone(), Some(writer_props)).unwrap()
// }
