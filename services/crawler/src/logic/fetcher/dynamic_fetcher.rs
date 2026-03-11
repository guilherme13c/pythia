use super::{FetchResult, Fetcher};
use async_trait::async_trait;
use headless_chrome::Browser;
use reqwest::Client;

pub struct DynamicFetcher {
    pub browserless_url: String,
    pub http_client: Client,
}

#[async_trait]
impl Fetcher for DynamicFetcher {
    async fn fetch(&self, url: &str) -> FetchResult {
        let u = url.to_string();
        let ws_url = self.browserless_url.clone();

        let res = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, String> {
            let browser = Browser::connect(ws_url)
                .map_err(|e| format!("Failed to connect to browserless: {}", e))?;

            let tab = browser.new_tab().map_err(|e| e.to_string())?;
            tab.navigate_to(&u).map_err(|e| e.to_string())?;
            tab.wait_until_navigated().map_err(|e| e.to_string())?;
            std::thread::sleep(std::time::Duration::from_secs(2));

            tab.get_content()
                .map(|html| html.into_bytes())
                .map_err(|e| e.to_string())
        })
        .await;

        match res {
            Ok(Ok(bytes)) => FetchResult::Success {
                content: bytes,
                mime_type: "text/html".to_string(),
            },
            Ok(Err(e)) => FetchResult::Error(e),
            Err(e) => FetchResult::Error(format!("Tokio spawn_blocking error: {}", e)),
        }
    }

    async fn fetch_robots(&self, url: &str) -> Option<String> {
        match self.http_client.get(url).send().await {
            Ok(response) if response.status().is_success() => response.text().await.ok(),
            _ => None,
        }
    }
}
