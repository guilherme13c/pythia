use std::env;

#[derive(Debug, Clone)]
pub struct QueryConfig {
    pub port: u16,
    pub processor_url: String,
    pub indexer_url: String,
    pub otlp_endpoint: Option<String>,
}

impl QueryConfig {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();

        Self {
            port: env::var("PORT")
                .unwrap_or_else(|_| "4000".to_string())
                .parse()
                .unwrap_or(4000),
            processor_url: env::var("PROCESSOR_URL")
                .unwrap_or_else(|_| "http://localhost:3001".to_string()),
            indexer_url: env::var("INDEXER_URL")
                .unwrap_or_else(|_| "http://localhost:3002".to_string()),
            otlp_endpoint: env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok(),
        }
    }
}
