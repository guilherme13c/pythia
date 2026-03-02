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
    fn handle_add_urls(&self, state: &mut ManagerState, urls: Vec<String>) {
        for url in urls {
            if !state.seen_urls.check(&url) {
                state.seen_urls.set(&url);

                state.frontier.push_back(url.clone());

                if let Err(e) = state.db.execute(
                    "INSERT INTO urls (url, status) VALUES (?1, 'pending') ON CONFLICT(url) DO NOTHING",
                    [&url],
                ) {
                    warn!("Failed to persist URL to DB: {}", e);
                }
            }
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

    fn handle_request_work(&self, state: &mut ManagerState, worker_ref: ActorRef<WorkerMessage>) {
        let mut next_job = None;
        let mut skipped_urls = Vec::new();
        let queue_len = state.frontier.len();

        for _ in 0..queue_len {
            if let Some(url_str) = state.frontier.pop_front() {
                let parsed_url = match Url::parse(&url_str) {
                    Ok(u) => u,
                    Err(e) => {
                        warn!("Failed to parse URL '{}': {}", url_str, e);
                        continue;
                    }
                };

                let domain = match parsed_url.host_str() {
                    Some(d) => d.to_string(),
                    _ => continue,
                };

                let path = parsed_url.path();
                let scheme = parsed_url.scheme();

                let metadata = state
                    .domain_metadata
                    .entry(domain.clone())
                    .or_insert_with(DomainMetadata::default_unfetched);

                if !metadata.rules_fetched {
                    let robots_url = format!("{}://{}/robots.txt", scheme, domain);
                    skipped_urls.push(url_str);
                    metadata.rules_fetched = true;

                    next_job = Some(WorkerMessage::FetchRobotsTxt {
                        domain,
                        url: robots_url,
                    });
                    break;
                }

                if !metadata.can_crawl(path) {
                    debug!("Dropped URL due to robots.txt Disallow: {}", url_str);
                    continue;
                }

                let now = Instant::now();

                if let Some(backoff_time) = metadata.backoff_until {
                    if now < backoff_time {
                        skipped_urls.push(url_str);
                        continue;
                    } else {
                        metadata.backoff_until = None;
                    }
                }

                let last_hit = metadata
                    .last_hit
                    .unwrap_or_else(|| now - (metadata.crawl_delay * 2));

                if now.duration_since(last_hit) >= metadata.crawl_delay {
                    metadata.last_hit = Some(now);
                    next_job = Some(WorkerMessage::Fetch(url_str));
                    break;
                } else {
                    skipped_urls.push(url_str);
                }
            }
        }

        for skipped in skipped_urls.into_iter().rev() {
            state.frontier.push_front(skipped);
        }

        if let Some(job) = next_job {
            let _ = worker_ref.cast(job);
        } else {
            let _ = worker_ref.cast(WorkerMessage::NoWorkAvailable);
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

        state.frontier.push_front(url);

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
        _myself: ActorRef<Self::Msg>,
        shard_idx: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Crawler Manager Shard starting...");
        Ok(ManagerState::new(shard_idx))
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ManagerMessage::AddUrls(urls) => {
                self.handle_add_urls(state, urls);
            }
            ManagerMessage::UpdateDomainRules { domain, metadata } => {
                self.handle_update_domain_rules(state, domain, metadata);
            }
            ManagerMessage::RequestWork(worker_ref) => {
                self.handle_request_work(state, worker_ref);
            }
            ManagerMessage::DomainRateLimited { domain, url } => {
                self.handle_rate_limited(state, domain, url);
            }
            ManagerMessage::CrawlSuccess { domain, url } => {
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

        assert_eq!(state.frontier.len(), 2);
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

        assert_eq!(state.frontier.len(), 3);
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
}
