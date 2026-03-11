use super::{FetchResult, Fetcher};
use async_trait::async_trait;
use reqwest::Client;

pub struct StaticFetcher {
    pub http_client: Client,
}

#[async_trait]
impl Fetcher for StaticFetcher {
    async fn fetch(&self, url: &str) -> FetchResult {
        let resp = match self.http_client.get(url).send().await {
            Ok(r) => r,
            Err(e) => return FetchResult::Error(e.to_string()),
        };

        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return FetchResult::RateLimited;
        }

        if !resp.status().is_success() {
            return FetchResult::Error(format!("HTTP Status: {}", resp.status()));
        }

        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|val| val.to_str().ok())
            .unwrap_or("text/html")
            .to_lowercase();

        match resp.bytes().await {
            Ok(bytes) => FetchResult::Success {
                content: bytes.to_vec(),
                mime_type: content_type,
            },
            Err(e) => FetchResult::Error(format!("Failed to read bytes: {}", e)),
        }
    }

    async fn fetch_robots(&self, url: &str) -> Option<String> {
        match self.http_client.get(url).send().await {
            Ok(response) if response.status().is_success() => response.text().await.ok(),
            _ => None,
        }
    }
}
