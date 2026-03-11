pub mod dynamic_fetcher;
pub mod static_fetcher;

use async_trait::async_trait;

pub enum FetchResult {
    Success { content: Vec<u8>, mime_type: String },
    RateLimited,
    Error(String),
}

#[async_trait]
pub trait Fetcher: Send + Sync {
    async fn fetch(&self, url: &str) -> FetchResult;

    async fn fetch_robots(&self, url: &str) -> Option<String>;
}
