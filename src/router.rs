use actix_web::{get, http::Error, post, web, HttpResponse, Responder};

use serde_derive::{Deserialize, Serialize};

use crate::app;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MeltResponse {
    pub code: u16,
    pub message: String,
    pub error_detail: Option<String>,
}

#[post("/{name}/_bulk")]
pub async fn bulk(
    app: web::Data<app::AppState>,
    name: web::Path<String>,
    body: web::Bytes,
) -> Result<HttpResponse, Error> {
    let name: String = name.into_inner();
    let service = app.service();
    let res = service.bulk(&name, body).await;
    match res {
        Ok(v) => Ok(HttpResponse::Ok().json(v)),
        Err(e) => {
            log::error!("Error process request {:?}", e);
            Ok(HttpResponse::BadRequest().json(()))
        }
    }
}

#[get("/status")]
async fn status(app: web::Data<app::AppState>) -> impl Responder {
    log::info!("status: {} ", app.app_name());
    "ok"
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};

    // use super::*;

    fn test_schema() -> Arc<Schema> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Boolean, false);
        let mut builder = SchemaBuilder::new();
        builder.push(field_a);
        builder.push(field_b);
        Arc::new(builder.finish())
    }
    #[test]
    fn test_name() {
        test_schema();
    }
}
