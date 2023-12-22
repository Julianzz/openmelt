use env_logger::Env;
use melt::*;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // println!("Hello, world!");
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    log::set_max_level(log::LevelFilter::Info);

    server::start_server("127.0.0.1", 8080, "data").await
}
