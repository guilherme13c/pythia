use headless_chrome::Browser;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use reqwest::Client;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::{error, info};
use url::Url;

use crate::communication::publisher::{DocumentMessage, DocumentPublisher};
use crate::data::blob_storage::BlobStorage;
use crate::logic::extract;
use crate::logic::manager::ManagerMessage;

pub enum WorkerMessage {
    Fetch(String),
    FetchRobotsTxt(String, String),
}

pub enum WorkerType {
    Static,
    Dynamic(Arc<Browser>),
}

pub struct WorkerState {
    pub http_client: Client,
    pub blob_storage: Arc<dyn BlobStorage>,
    pub publisher: Arc<dyn DocumentPublisher>,
    pub worker_type: WorkerType,
    pub shard_idx: usize,
    pub total_shards: usize,
}

pub fn get_shard_index(domain: &str, num_shards: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    domain.hash(&mut hasher);
    (hasher.finish() as usize) % num_shards
}

enum FetchResult {
    Success { content: Vec<u8>, mime_type: String },
    RateLimited,
    Error(String),
}

pub struct WorkerActor;

impl WorkerActor {
    fn send_to_manager_shard(&self, shard_idx: usize, msg: ManagerMessage) {
        if let Some(manager_cell) = ractor::registry::where_is(format!("manager-{}", shard_idx)) {
            let manager_ref: ActorRef<ManagerMessage> = manager_cell.into();
            let _ = manager_ref.cast(msg);
        }
    }

    async fn execute_fetch(&self, state: &WorkerState, url: &str) -> FetchResult {
        match &state.worker_type {
            WorkerType::Static => self.fetch_static(&state.http_client, url).await,
            WorkerType::Dynamic(browser) => self.fetch_dynamic(browser, url).await,
        }
    }

    async fn fetch_static(&self, client: &Client, url: &str) -> FetchResult {
        let resp = match client.get(url).send().await {
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

    async fn fetch_dynamic(&self, browser: &Arc<Browser>, url: &str) -> FetchResult {
        let b = browser.clone();
        let u = url.to_string();

        let res = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, String> {
            let tab = b.new_tab().map_err(|e| e.to_string())?;
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

    async fn handle_fetch(
        &self,
        state: &mut WorkerState,
        myself: ActorRef<WorkerMessage>,
        url: String,
    ) {
        let mode_str = match state.worker_type {
            WorkerType::Static => "Static",
            WorkerType::Dynamic(_) => "Dynamic",
        };
        info!("[Logic Layer] Worker fetching: {} ({})", url, mode_str);

        let domain = Url::parse(&url)
            .map(|u| u.host_str().unwrap_or("").to_string())
            .unwrap_or_default();

        match self.execute_fetch(state, &url).await {
            FetchResult::Success { content, mime_type } => {
                self.process_successful_fetch(state, &url, &domain, content, mime_type)
                    .await;
            }
            FetchResult::RateLimited => {
                let shard = get_shard_index(&domain, state.total_shards);
                self.send_to_manager_shard(shard, ManagerMessage::DomainRateLimited(domain, url));
            }
            FetchResult::Error(e) => {
                error!("Failed to fetch {}: {:?}", url, e);
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        self.send_to_manager_shard(
            state.shard_idx,
            ManagerMessage::RequestWork(myself.get_name().unwrap()),
        );
    }

    async fn process_successful_fetch(
        &self,
        state: &WorkerState,
        url: &str,
        domain: &str,
        content: Vec<u8>,
        mime_type: String,
    ) {
        let links = extract::extract_links(&content, &mime_type, url);
        if !links.is_empty() {
            self.route_extracted_links(state, links);
        }

        match state.blob_storage.save_blob(content).await {
            Ok(blob_id) => {
                let msg = DocumentMessage {
                    url: url.to_string(),
                    blob_id,
                    mime_type,
                };
                let _ = state.publisher.publish(msg).await;
                let shard = get_shard_index(domain, state.total_shards);
                self.send_to_manager_shard(
                    shard,
                    ManagerMessage::CrawlSuccess(domain.to_string(), url.to_string()),
                );
            }
            Err(e) => error!("Failed to save blob: {}", e),
        }
    }

    fn route_extracted_links(&self, state: &WorkerState, links: Vec<String>) {
        let mut routed_batches: HashMap<usize, Vec<String>> = HashMap::new();
        for link in links {
            if let Ok(parsed) = Url::parse(&link) {
                let link_domain = parsed.host_str().unwrap_or("").to_string();
                if !link_domain.is_empty() {
                    let shard = get_shard_index(&link_domain, state.total_shards);
                    routed_batches.entry(shard).or_default().push(link);
                }
            }
        }

        for (shard, urls) in routed_batches {
            self.send_to_manager_shard(shard, ManagerMessage::AddUrls(urls));
        }
    }

    async fn handle_fetch_robots(
        &self,
        state: &mut WorkerState,
        myself: ActorRef<WorkerMessage>,
        domain: String,
        url: String,
    ) {
        info!("[Logic Layer] Fetching rules for: {}", domain);
        let robots_txt = match state.http_client.get(&url).send().await {
            Ok(response) if response.status().is_success() => response.text().await.ok(),
            _ => None,
        };

        let shard = get_shard_index(&domain, state.total_shards);
        self.send_to_manager_shard(
            shard,
            ManagerMessage::UpdateDomainRules(domain.clone(), robots_txt),
        );
        self.send_to_manager_shard(
            state.shard_idx,
            ManagerMessage::RequestWork(myself.get_name().unwrap()),
        );
    }
}

impl Actor for WorkerActor {
    type Msg = WorkerMessage;
    type State = WorkerState;
    type Arguments = WorkerState;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(state)
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            WorkerMessage::Fetch(url) => self.handle_fetch(state, myself, url).await,
            WorkerMessage::FetchRobotsTxt(domain, url) => {
                self.handle_fetch_robots(state, myself, domain, url).await
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::communication::publisher::MockPublisher;
    use crate::data::blob_storage::MockBlobStorage;
    use axum::{Router, routing::get};
    use reqwest::StatusCode;
    use tokio::net::TcpListener;

    async fn spawn_test_server() -> String {
        let app = Router::new()
            .route("/success", get(|| async { "<html>Success!</html>" }))
            .route(
                "/rate-limit",
                get(|| async { (StatusCode::TOO_MANY_REQUESTS, "Slow down!") }),
            )
            .route(
                "/server-error",
                get(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "Boom!") }),
            );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{}", addr);

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        url
    }

    fn create_test_worker_state() -> WorkerState {
        WorkerState {
            http_client: Client::new(),
            blob_storage: Arc::new(MockBlobStorage),
            publisher: Arc::new(MockPublisher),
            worker_type: WorkerType::Static,
            shard_idx: 0,
            total_shards: 1,
        }
    }

    #[tokio::test]
    async fn test_execute_fetch_success() {
        let base_url = spawn_test_server().await;
        let worker = WorkerActor;
        let state = create_test_worker_state();

        let target_url = format!("{}/success", base_url);
        let result = worker.execute_fetch(&state, &target_url).await;

        match result {
            FetchResult::Success { content, mime_type } => {
                let html = String::from_utf8(content).unwrap();
                assert_eq!(html, "<html>Success!</html>");
                assert!(mime_type.contains("text/html") || mime_type.contains("text/plain"));
            }
            _ => panic!("Expected Success, got something else"),
        }
    }

    #[tokio::test]
    async fn test_execute_fetch_rate_limited() {
        let base_url = spawn_test_server().await;
        let worker = WorkerActor;
        let state = create_test_worker_state();

        let target_url = format!("{}/rate-limit", base_url);
        let result = worker.execute_fetch(&state, &target_url).await;

        match result {
            FetchResult::RateLimited => {}
            _ => panic!("Expected RateLimited, got something else"),
        }
    }

    #[tokio::test]
    async fn test_execute_fetch_server_error() {
        let base_url = spawn_test_server().await;
        let worker = WorkerActor;
        let state = create_test_worker_state();

        let target_url = format!("{}/server-error", base_url);
        let result = worker.execute_fetch(&state, &target_url).await;

        match result {
            FetchResult::Error(msg) => assert!(msg.contains("500 Internal Server Error")),
            _ => panic!("Expected Error, got something else"),
        }
    }

    #[tokio::test]
    async fn test_execute_fetch_network_failure() {
        let worker = WorkerActor;
        let state = create_test_worker_state();

        let target_url = "http://127.0.0.1:1";
        let result = worker.execute_fetch(&state, target_url).await;

        match result {
            FetchResult::Error(msg) => {
                println!("Caught expected network error: {}", msg);
            }
            _ => panic!("Expected Error, got something else"),
        }
    }
}
