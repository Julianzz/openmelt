use crate::*;
use actix_web::{web, App, HttpServer};

pub async fn start_server(addr: &str, port: u16, root: &str) -> std::io::Result<()> {
    // let app =
    let root = root.to_owned();
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(app::AppState::new("openmelt", &root)))
            .service(router::status)
            .service(router::bulk)
    })
    .bind((addr, port))?
    .run()
    .await
}
