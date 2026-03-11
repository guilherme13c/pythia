use crate::communication::publisher::{DocumentMessage, DocumentPublisher};
use crate::data::blob_storage::BlobStorage;
use crate::logic::extract;
use crate::logic::fetcher::{FetchResult, Fetcher};
use crate::logic::manager::ManagerMessage;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::{debug, error, info};
use url::Url;

#[derive(Debug)]
pub enum WorkerMessage {
    Fetch(String),
    FetchRobotsTxt(String, String),
    NoWork,
}

pub enum WorkerType {
    Static,
    Dynamic,
}

pub struct WorkerState {
    pub fetcher: Box<dyn Fetcher>,
    pub blob_storage: Arc<dyn BlobStorage>,
    pub publisher: Arc<dyn DocumentPublisher>,
    pub shard_idx: usize,
    pub total_shards: usize,
}

pub fn get_shard_index(domain: &str, num_shards: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    domain.hash(&mut hasher);
    (hasher.finish() as usize) % num_shards
}

pub struct WorkerActor;

impl WorkerActor {
    fn send_to_manager_shard(&self, shard_idx: usize, msg: ManagerMessage) {
        if let Some(manager_cell) = ractor::registry::where_is(format!("manager-{}", shard_idx)) {
            let manager_ref: ActorRef<ManagerMessage> = manager_cell.into();
            let _ = manager_ref.cast(msg);
        }
    }

    async fn handle_fetch(
        &self,
        state: &mut WorkerState,
        myself: ActorRef<WorkerMessage>,
        url: String,
    ) {
        info!("[Logic Layer] Worker fetching: {}", url);

        let domain = Url::parse(&url)
            .map(|u| u.host_str().unwrap_or("").to_string())
            .unwrap_or_default();

        match state.fetcher.fetch(&url).await {
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

        let robots_txt = state.fetcher.fetch_robots(&url).await;
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

    async fn handle_no_work(&self, state: &mut WorkerState, myself: ActorRef<WorkerMessage>) {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
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
        debug!("Received message: {:?}", message);

        match message {
            WorkerMessage::Fetch(url) => self.handle_fetch(state, myself, url).await,
            WorkerMessage::FetchRobotsTxt(domain, url) => {
                self.handle_fetch_robots(state, myself, domain, url).await
            }
            WorkerMessage::NoWork => self.handle_no_work(state, myself).await,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::blob_storage::DbBlobStorage;
    use async_trait::async_trait;
    use std::pin::Pin;

    struct MockPublisher;

    impl DocumentPublisher for MockPublisher {
        fn publish(
            &self,
            _message: DocumentMessage,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct MockFetcher;

    #[async_trait]
    impl Fetcher for MockFetcher {
        async fn fetch(&self, url: &str) -> FetchResult {
            if url.contains("success") {
                FetchResult::Success {
                    content: b"<html>Success!</html>".to_vec(),
                    mime_type: "text/html".to_string(),
                }
            } else if url.contains("rate-limit") {
                FetchResult::RateLimited
            } else {
                FetchResult::Error("Network Error".to_string())
            }
        }

        async fn fetch_robots(&self, _url: &str) -> Option<String> {
            Some("User-agent: *\nAllow: /".to_string())
        }
    }

    fn create_test_worker_state() -> WorkerState {
        WorkerState {
            fetcher: Box::new(MockFetcher),
            blob_storage: Arc::new(DbBlobStorage::new(":memory:").unwrap()),
            publisher: Arc::new(MockPublisher),
            shard_idx: 0,
            total_shards: 1,
        }
    }

    #[tokio::test]
    async fn test_fetcher_success() {
        let state = create_test_worker_state();
        let target_url = "http://example.com/success";

        let result = state.fetcher.fetch(target_url).await;

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
    async fn test_fetcher_rate_limited() {
        let state = create_test_worker_state();
        let target_url = "http://example.com/rate-limit";

        let result = state.fetcher.fetch(target_url).await;

        match result {
            FetchResult::RateLimited => {}
            _ => panic!("Expected RateLimited, got something else"),
        }
    }

    #[tokio::test]
    async fn test_fetcher_server_error() {
        let state = create_test_worker_state();
        let target_url = "http://example.com/server-error";

        let result = state.fetcher.fetch(target_url).await;

        match result {
            FetchResult::Error(msg) => assert!(msg.contains("Network Error")),
            _ => panic!("Expected Error, got something else"),
        }
    }

    #[tokio::test]
    async fn test_fetcher_network_failure() {
        let state = create_test_worker_state();
        let target_url = "http://127.0.0.1:1/error";

        let result = state.fetcher.fetch(target_url).await;

        match result {
            FetchResult::Error(msg) => {
                assert!(msg.contains("Network Error"));
            }
            _ => panic!("Expected Error, got something else"),
        }
    }
}
