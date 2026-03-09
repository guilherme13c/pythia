use ractor::concurrency::Duration;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::time::Instant;
use tracing::{debug, info, warn};
use url::Url;

use super::messages::ManagerMessage;
use super::state::{DomainMetadata, ManagerState};
use crate::actors::crawler::worker::messages::WorkerMessage;

pub struct ManagerActor;

impl ManagerActor {
    pub fn handle_add_urls(&self, state: &mut ManagerState, urls: Vec<String>) {
        let mut new_urls = Vec::new();
        for url in urls {
            if !state.seen_urls.check(&url) {
                state.seen_urls.set(&url);

                if crate::actors::crawler::manager::state::is_spa_url(&url) {
                    state.dynamic_frontier.push_back(url.clone());
                } else {
                    state.static_frontier.push_back(url.clone());
                }

                new_urls.push(url);
            }
        }
        if new_urls.is_empty() {
            return;
        }

        let tx = match state.db.transaction() {
            Ok(t) => t,
            Err(e) => {
                warn!("Failed to start transaction: {}", e);
                return;
            }
        };

        {
            let mut stmt = tx.prepare("INSERT INTO urls (url, status) VALUES (?1, 'pending') ON CONFLICT(url) DO NOTHING").unwrap();
            for url in new_urls {
                let _ = stmt.execute([&url]);
            }
        }

        if let Err(e) = tx.commit() {
            warn!("Failed to commit batch URL inserts: {}", e);
        }
    }

    fn handle_update_domain_rules(
        &self,
        state: &mut ManagerState,
        domain: String,
        metadata: DomainMetadata,
    ) {
        state.domain_metadata.insert(domain, metadata);
    }

    pub fn handle_request_work(&self, state: &mut ManagerState, worker_name: String) {
        let is_dynamic_worker = worker_name.starts_with("dynamic-worker");
        let mut next_job = None;
        let mut skipped_urls = Vec::new();

        let queue_len = if is_dynamic_worker {
            state.dynamic_frontier.len()
        } else {
            state.static_frontier.len()
        };

        for _ in 0..queue_len {
            let url_opt = if is_dynamic_worker {
                state.dynamic_frontier.pop_front()
            } else {
                state.static_frontier.pop_front()
            };

            if let Some(url_str) = url_opt {
                if let Some(job) = self.evaluate_url(state, &url_str, &mut skipped_urls) {
                    next_job = Some(job);
                    break;
                }
            } else {
                break;
            }
        }

        for skipped in skipped_urls.into_iter().rev() {
            if is_dynamic_worker {
                state.dynamic_frontier.push_front(skipped);
            } else {
                state.static_frontier.push_front(skipped);
            }
        }

        self.dispatch_job(worker_name, next_job);
    }

    fn evaluate_url(
        &self,
        state: &mut ManagerState,
        url_str: &str,
        skipped_urls: &mut Vec<String>,
    ) -> Option<WorkerMessage> {
        let parsed_url = Url::parse(url_str).ok()?;
        let domain = parsed_url.host_str()?.to_string();

        let metadata = state
            .domain_metadata
            .entry(domain.clone())
            .or_insert_with(DomainMetadata::default_unfetched);

        if !metadata.rules_fetched {
            metadata.rules_fetched = true;
            skipped_urls.push(url_str.to_string());
            let robots_url = format!("{}://{}/robots.txt", parsed_url.scheme(), domain);
            return Some(WorkerMessage::FetchRobotsTxt(domain, robots_url));
        }

        if !metadata.can_crawl(parsed_url.path()) {
            debug!("Dropped URL due to robots.txt Disallow: {}", url_str);
            return None;
        }

        let now = Instant::now();

        if let Some(backoff_time) = metadata.backoff_until {
            if now < backoff_time {
                skipped_urls.push(url_str.to_string());
                return None;
            }
            metadata.backoff_until = None;
        }

        let last_hit = metadata
            .last_hit
            .unwrap_or_else(|| now - (metadata.crawl_delay * 2));
        if now.duration_since(last_hit) >= metadata.crawl_delay {
            metadata.last_hit = Some(now);
            return Some(WorkerMessage::Fetch(url_str.to_string()));
        }

        skipped_urls.push(url_str.to_string());
        None
    }

    fn dispatch_job(&self, worker_name: String, job: Option<WorkerMessage>) {
        if let Some(worker_cell) = ractor::registry::where_is(worker_name) {
            let worker_ref: ActorRef<WorkerMessage> = worker_cell.into();
            let msg = job.unwrap_or(WorkerMessage::NoWorkAvailable);
            let _ = worker_ref.cast(msg);
        }
    }

    fn handle_rate_limited(&self, state: &mut ManagerState, domain: String, url: String) {
        let metadata = state
            .domain_metadata
            .entry(domain.clone())
            .or_insert_with(DomainMetadata::default_unfetched);

        metadata.consecutive_errors += 1;

        let base_delay = std::cmp::max(1, metadata.crawl_delay.as_secs());
        let backoff_secs = std::cmp::min(300, base_delay * 2_u64.pow(metadata.consecutive_errors));

        metadata.backoff_until = Some(Instant::now() + Duration::from_secs(backoff_secs));

        if crate::actors::crawler::manager::state::is_spa_url(&url) {
            state.dynamic_frontier.push_front(url);
        } else {
            state.static_frontier.push_front(url);
        }

        warn!(
            "Domain {} gave a 429! Backing off for {} seconds.",
            domain, backoff_secs
        );
    }

    fn handle_crawl_success(&self, state: &mut ManagerState, domain: String, url: String) {
        if let Some(metadata) = state.domain_metadata.get_mut(&domain) {
            metadata.consecutive_errors = 0;
            metadata.backoff_until = None;
        }
        if let Err(e) = state
            .db
            .execute("UPDATE urls SET status = 'done' WHERE url = ?1", [&url])
        {
            warn!("Failed to update URL status in DB: {}", e);
        }
    }
}

impl Actor for ManagerActor {
    type Msg = ManagerMessage;
    type State = ManagerState;
    type Arguments = usize;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        shard_idx: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        ractor::pg::join("crawler_managers".to_string(), vec![myself.clone().into()]);
        ractor::pg::join(
            format!("manager-shard-{}", shard_idx),
            vec![myself.clone().into()],
        );

        info!("Crawler Manager Shard starting...");

        Ok(ManagerState::new(shard_idx))
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!("received message: {:?}", message);
        match message {
            ManagerMessage::AddUrls(urls) => {
                self.handle_add_urls(state, urls);
            }
            ManagerMessage::UpdateDomainRules(domain, robots_txt) => {
                let metadata = if let Some(txt) = robots_txt {
                    crate::actors::crawler::worker::common::parse_robots_txt(&txt, &domain)
                } else {
                    let mut m = DomainMetadata::default_unfetched();
                    m.rules_fetched = true;
                    m
                };
                self.handle_update_domain_rules(state, domain, metadata);
            }
            ManagerMessage::RequestWork(worker_ref) => {
                self.handle_request_work(state, worker_ref);
            }
            ManagerMessage::DomainRateLimited(domain, url) => {
                self.handle_rate_limited(state, domain, url);
            }
            ManagerMessage::CrawlSuccess(domain, url) => {
                self.handle_crawl_success(state, domain, url);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_add_urls_deduplication() {
        let manager = ManagerActor;
        let mut state = ManagerState::in_memory();

        let new_urls = vec![
            "https://example.com/page1".to_string(),
            "https://example.com/page2".to_string(),
            "https://example.com/page1".to_string(),
        ];

        manager.handle_add_urls(&mut state, new_urls);

        assert_eq!(state.static_frontier.len(), 2);
        assert!(
            state
                .seen_urls
                .check(&"https://example.com/page1".to_string())
        );
        assert!(
            state
                .seen_urls
                .check(&"https://example.com/page2".to_string())
        );
    }

    #[test]
    fn test_handle_rate_limited_exponential_backoff() {
        let manager = ManagerActor;
        let mut state = ManagerState::in_memory();
        let domain = "wikipedia.org".to_string();
        let url = "https://wikipedia.org/page1".to_string();

        manager.handle_rate_limited(&mut state, domain.clone(), url.clone());

        let metadata = state.domain_metadata.get(&domain).unwrap();
        assert_eq!(metadata.consecutive_errors, 1);

        manager.handle_rate_limited(&mut state, domain.clone(), url.clone());
        let metadata = state.domain_metadata.get(&domain).unwrap();
        assert_eq!(metadata.consecutive_errors, 2);

        manager.handle_rate_limited(&mut state, domain.clone(), url.clone());
        let metadata = state.domain_metadata.get(&domain).unwrap();
        assert_eq!(metadata.consecutive_errors, 3);

        assert_eq!(state.static_frontier.len(), 3);
    }

    #[test]
    fn test_handle_crawl_success_clears_penalty() {
        let manager = ManagerActor;
        let mut state = ManagerState::in_memory();
        let domain = "wikipedia.org".to_string();
        let url = "https://wikipedia.org/page1".to_string();

        manager.handle_rate_limited(&mut state, domain.clone(), url.clone());
        assert_eq!(
            state
                .domain_metadata
                .get(&domain)
                .unwrap()
                .consecutive_errors,
            1
        );
        assert!(
            state
                .domain_metadata
                .get(&domain)
                .unwrap()
                .backoff_until
                .is_some()
        );

        manager.handle_crawl_success(&mut state, domain.clone(), url.clone());

        let metadata = state.domain_metadata.get(&domain).unwrap();
        assert_eq!(metadata.consecutive_errors, 0);
        assert!(metadata.backoff_until.is_none());
    }

    #[test]
    fn test_evaluate_url_rules_unfetched() {
        let manager = ManagerActor;
        let mut state = ManagerState::in_memory();
        let mut skipped = Vec::new();

        let job = manager.evaluate_url(&mut state, "https://example.com/page", &mut skipped);

        match job {
            Some(WorkerMessage::FetchRobotsTxt(domain, url)) => {
                assert_eq!(domain, "example.com");
                assert_eq!(url, "https://example.com/robots.txt");
            }
            _ => panic!("Expected FetchRobotsTxt"),
        }

        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0], "https://example.com/page");
    }

    #[test]
    fn test_evaluate_url_disallowed() {
        let manager = ManagerActor;
        let mut state = ManagerState::in_memory();
        let mut skipped = Vec::new();

        let mut metadata = DomainMetadata::default_unfetched();
        metadata.rules_fetched = true;
        metadata.disallowed_paths.push("/private".to_string());
        state
            .domain_metadata
            .insert("example.com".to_string(), metadata);

        let job = manager.evaluate_url(&mut state, "https://example.com/private/doc", &mut skipped);

        assert!(job.is_none());
        assert!(skipped.is_empty());
    }

    #[test]
    fn test_evaluate_url_ready_to_fetch() {
        let manager = ManagerActor;
        let mut state = ManagerState::in_memory();
        let mut skipped = Vec::new();

        let mut metadata = DomainMetadata::default_unfetched();
        metadata.rules_fetched = true;
        metadata.last_hit = Some(tokio::time::Instant::now() - std::time::Duration::from_secs(100));
        state
            .domain_metadata
            .insert("example.com".to_string(), metadata);

        let job = manager.evaluate_url(&mut state, "https://example.com/page", &mut skipped);

        match job {
            Some(WorkerMessage::Fetch(url)) => {
                assert_eq!(url, "https://example.com/page");
            }
            _ => panic!("Expected Fetch"),
        }
        assert!(skipped.is_empty());
    }
}
