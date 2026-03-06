use headless_chrome::Browser;
use reqwest::Client;
use std::sync::Arc;

pub struct DynamicWorkerState {
    pub browser: Arc<Browser>,
    pub http_client: Client,
    pub shard_idx: usize,
}
