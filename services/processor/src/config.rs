use std::env;

#[derive(Debug, Clone)]
pub struct ProcessorConfig {
    pub port: u16,
    pub cache_path: String,
    pub blob_db_path: String,
    pub amqp_addr: String,
    pub otlp_endpoint: Option<String>,
}

impl ProcessorConfig {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();

        Self {
            port: env::var("PROCESSOR_PORT")
                .unwrap_or_else(|_| "3001".to_string())
                .parse()
                .unwrap_or(3001),
            cache_path: env::var("FASTEMBED_CACHE_PATH")
                .unwrap_or_else(|_| ".local_models/fastembed".to_string()),
            blob_db_path: std::env::var("BLOB_DB_PATH")
                .unwrap_or_else(|_| "data/blobs.db".to_string()),
            amqp_addr: std::env::var("AMQP_ADDR")
                .unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".to_string()),
            otlp_endpoint: env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok(),
        }
    }
}
