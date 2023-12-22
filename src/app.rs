use crate::{ingest::IngestService, storage::Storage};

// #[derive(Clone,Copy)]
pub struct AppState {
    app_name: String,
    service: IngestService,
}

impl AppState {
    pub fn new(name: &str, root: &str) -> AppState {
        let storage = Storage::new(root);
        let service = IngestService::new(storage);
        AppState {
            app_name: name.to_owned(),
            service,
        }
    }
    pub fn app_name(&self) -> &str {
        &self.app_name
    }
    pub fn service(&self) -> &IngestService {
        &self.service
    }
}
