use actix_web::web::Bytes;
use env_logger::Env;
use melt::{storage::Storage, *};

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    log::set_max_level(log::LevelFilter::Info);

    let data = r#"{"a": 1, "b": 1}"#;
    let data = Bytes::from(data);
    let storage = Storage::new("./datas/");
    let service = ingest::IngestService::new(storage);

    match service.bulk("logs", data).await {
        Ok(_) => println!("write success"),
        Err(err) => println!("{}", err.backtrace()),
    }
}
