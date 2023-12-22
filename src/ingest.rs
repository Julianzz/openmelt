use std::io::{BufRead, BufReader};

use actix_web::web::*;
use ahash::AHashMap;
use anyhow::*;
use chrono::prelude::*;

use crate::{
    arrow, id_gen::gen_id, json_utls::parse_timestamp, parquet_utils, schema, schema::MeltSchema,
    storage::Storage,
};

use serde_json::Value;

static TIMPSTAMP_FIELD_NAME: &str = "timestamp";
static ARROW_EXT: &str = "parquet";
static SCHEMA_EXT: &str = "schema";
pub struct IngestService {
    storage: Storage,
}
impl IngestService {
    pub fn new(storage: Storage) -> IngestService {
        IngestService { storage }
    }

    pub fn ensure_dir(&self, table_name: &str, partition: &str) -> Result<String, anyhow::Error> {
        let path = format!("{table_name}/{partition}");
        std::fs::create_dir_all(&path)?;
        Ok(path)
    }

    pub fn parse_line_file(&self, body: Bytes) -> Result<Vec<Value>, anyhow::Error> {
        let reader = BufReader::new(body.as_ref());
        let mut records = Vec::new();
        for line in reader.lines() {
            let line = line?;
            // log::info!("{}", line);
            if line.is_empty() {
                continue;
            }
            let value = serde_json::from_slice(line.as_bytes())?;
            // log::info!("{:?}", value);
            records.push(value);
        }
        Ok(records)
    }

    pub fn process_bulk_item(&self, val: Value) -> Result<(i64, Value), anyhow::Error> {
        let stamp = parse_timestamp(TIMPSTAMP_FIELD_NAME, &val)?;
        let mut val = val;
        let local_val = val
            .as_object_mut()
            .ok_or(anyhow!("format wrong, can not convert to map"))?;
        local_val.insert(TIMPSTAMP_FIELD_NAME.into(), Value::Number(stamp.into()));
        Ok((stamp, val))
    }

    pub async fn bulk(&self, table_name: &str, body: Bytes) -> Result<(), anyhow::Error> {
        let records = self.parse_line_file(body)?;
        self.ingest_(table_name, records).await
    }

    pub fn parquet_file_path(&self, partition: &str, name: &str) -> String {
        format!("{}/{}.{}", partition, name, ARROW_EXT)
    }
    pub fn schema_file_path(&self, partition: &str, name: &str) -> String {
        format!("{}/{}.{}", partition, name, SCHEMA_EXT)
    }

    pub fn get_partition_key(&self, stamp: i64) -> String {
        let time_key = Utc.timestamp_nanos(stamp * 1000);
        time_key.format("%Y-%m-%d-%H").to_string()
    }

    fn partition_records(
        &self,
        records: Vec<Value>,
    ) -> Result<AHashMap<String, Vec<Value>>, anyhow::Error> {
        let mut partitions: AHashMap<String, Vec<Value>> = AHashMap::new();
        for record in records.into_iter() {
            let (stamp, val) = self.process_bulk_item(record)?;
            let partition_key = self.get_partition_key(stamp);
            let entry = partitions.entry(partition_key).or_default();
            entry.push(val);
        }
        Ok(partitions)
    }

    async fn ingest_(&self, table_name: &str, records: Vec<Value>) -> Result<(), anyhow::Error> {
        let partitions = self.partition_records(records)?;
        for (partition, records) in partitions {
            //infer schema
            let schema = schema::infer_schema(&records)?;

            self.write_partition(table_name, &partition, &schema, records)
                .await?;
        }
        Ok(())
    }

    async fn write_partition(
        &self,
        table_name: &str,
        partition: &str,
        schema: &MeltSchema,
        records: Vec<Value>,
    ) -> Result<(), anyhow::Error> {
        let batch = arrow::json_to_recordbatch(schema, &records)?;

        let partition_path = format!("{table_name}/{partition}");
        self.storage.ensure_dir(&partition_path)?;
        let segment_id = gen_id();
        let filename = self.parquet_file_path(&partition_path, &segment_id);
        let mut datas = Vec::new();
        let mut writer = parquet_utils::new_parquet_writer(&mut datas, schema)?;
        // arrow::write_file_arrow(filename, &records, schema.as_ref()).await?
        writer.write(&batch)?;
        writer.close()?;

        self.storage.put(&filename, datas.into()).await?;

        //write schema
        let filename = self.schema_file_path(&partition_path, &segment_id);
        self.storage
            .put(&filename, schema.serialize()?.into())
            .await?;

        log::info!("schema :{}", schema.as_ref());

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::{storage::Storage, *};
    use actix_web::web::Bytes;
    use tempfile::*;

    #[tokio::test]
    async fn test_ingest_schema() {
        let data = r#"{"a": 1, "b": 1}"#;
        let name = "test";
        let data = Bytes::from(data);
        let tmp_dir = tempdir().unwrap();
        let storage = Storage::new(tmp_dir);
        let service = ingest::IngestService::new(storage);
        match service.bulk(name, data).await {
            Ok(_) => println!("write success"),
            Err(err) => println!("{}", err.backtrace()),
        }
    }
}
