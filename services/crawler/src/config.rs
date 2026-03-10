use std::env;

#[derive(Debug, Clone)]
pub struct CrawlerConfig {
    pub total_shards: usize,
    pub num_workers: usize,
    pub db_path: String,
    pub seed_urls: Vec<String>,
    pub blob_db_path: String,
    pub amqp_addr: String,
}

impl CrawlerConfig {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();

        Self {
            total_shards: env::var("CRAWLER_SHARDS")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),
            num_workers: env::var("CRAWLER_WORKERS")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),
            db_path: env::var("CRAWLER_DB_PATH").unwrap_or_else(|_| ":memory:".to_string()),
            seed_urls: env::var("CRAWLER_SEED_URLS")
                .unwrap_or_else(|_| "https://www.rust-lang.org/,https://tokio.rs/".to_string())
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            blob_db_path: std::env::var("BLOB_DB_PATH")
                .unwrap_or_else(|_| "data/blobs.db".to_string()),
            amqp_addr: std::env::var("AMQP_ADDR")
                .unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".to_string()),
        }
    }
}
