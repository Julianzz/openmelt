use crate::*;
use actix_web::{web, App, HttpServer};

pub async fn start_server(addr: &str, port: u16, root: &str) -> std::io::Result<()> {
    let root = root.to_owned();
    let service =web::Data::new(app::AppState::new("openmelt", &root));
    HttpServer::new(move || {
        App::new()
            .app_data(web::PayloadConfig::new(1024*1024*10))
            .app_data(service.clone())
            .service(router::status)
            .service(router::bulk)
            .service(router::search)
            .service(router::injest)
    })
    .bind((addr, port))?
    .run()
    .await
}
