use std::env;

#[derive(Debug, Clone)]
pub struct CrawlerConfig {
    pub total_shards: usize,
    pub num_workers: usize,
    pub db_path: String,
    pub seed_urls: Vec<String>,
}

impl CrawlerConfig {
    pub fn load() -> Self {
        // Loads the nearest .env file (service level), then falls back up to the root .env
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
        }
    }
}
