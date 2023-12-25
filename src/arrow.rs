use arrow_ipc::writer::StreamWriter;
use arrow_json::{writer::record_batches_to_json_rows, ReaderBuilder};
use datafusion::arrow::{self, datatypes::Schema, record_batch::RecordBatch};
use serde_json::map::Map as JsonMap;
use serde_json::Value;
use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::Arc,
};
use tokio::sync::RwLock;

pub struct RwFile {
    file: Option<RwLock<StreamWriter<File>>>,
}

impl RwFile {
    pub async fn write_arrow(&self, data: RecordBatch) -> Result<(), anyhow::Error> {
        let mut writer = self.file.as_ref().unwrap().write().await;
        writer.write(&data)?;
        Ok(())
    }
}

pub async fn get_or_create_arrow_writer(name: impl AsRef<Path>, schema: &Schema) -> Arc<RwFile> {
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(name)
        .unwrap();
    let f = StreamWriter::try_new(f, schema).unwrap();
    Arc::new(RwFile {
        file: Some(RwLock::new(f)),
    })
}

pub async fn write_file_arrow(
    file_name: impl AsRef<Path>,
    record: &[Value],
    schema: &Schema,
) -> Result<(), anyhow::Error> {
    let batch = json_to_recordbatch(schema, record)?;
    let file = get_or_create_arrow_writer(file_name, schema).await;
    file.write_arrow(batch).await?;
    Ok(())
}

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
