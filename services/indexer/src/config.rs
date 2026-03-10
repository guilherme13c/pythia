use std::env;

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub port: u16,
    pub lancedb_uri: String,
    pub lancedb_table: String,
    pub amqp_addr: String,
}

impl IndexerConfig {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();

        Self {
            port: env::var("INDEXER_PORT")
                .unwrap_or_else(|_| "3002".to_string())
                .parse()
                .unwrap_or(3002),
            lancedb_uri: env::var("LANCEDB_URI")
                .unwrap_or_else(|_| "data/lancedb_store".to_string()),
            lancedb_table: env::var("LANCEDB_TABLE").unwrap_or_else(|_| "documents".to_string()),
            amqp_addr: std::env::var("AMQP_ADDR")
                .unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".to_string()),
        }
    }
}
