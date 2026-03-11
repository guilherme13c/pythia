use std::env;

#[derive(Debug, Clone)]
pub struct CrawlerConfig {
    pub shard_index: usize,
    pub total_shards: usize,
    pub n_dynamic_workers: usize,
    pub n_static_workers: usize,
    pub db_path: String,
    pub blob_db_path: String,
    pub amqp_addr: String,
    pub browserless_url: String,
}

impl CrawlerConfig {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();

        Self {
            shard_index: env::var("HOSTNAME")
                .unwrap_or_else(|_| "crawler-0".to_string())
                .split('-')
                .last()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0),
            total_shards: env::var("CRAWLER_TOTAL_SHARDS")
                .unwrap_or_else(|_| "1".to_string())
                .parse()
                .unwrap_or(1),
            n_dynamic_workers: env::var("N_DYNAMIC_WORKERS")
                .unwrap_or_else(|_| "1".to_string())
                .parse()
                .unwrap_or(1),
            n_static_workers: env::var("N_STATIC_WORKERS")
                .unwrap_or_else(|_| "2".to_string())
                .parse()
                .unwrap_or(2),
            db_path: env::var("CRAWLER_DB_PATH")
                .unwrap_or_else(|_| "/data/frontier.db".to_string()),
            blob_db_path: std::env::var("BLOB_DB_PATH")
                .unwrap_or_else(|_| "/data/blobs.db".to_string()),
            amqp_addr: std::env::var("AMQP_ADDR")
                .unwrap_or_else(|_| "amqp://rabbitmq:5672/%2f".to_string()),
            browserless_url: std::env::var("BROWSERLESS_WS_URL")
                .unwrap_or_else(|_| "ws://browserless:3000".to_string()),
        }
    }
}
