use ractor::concurrency::Duration;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::time::Instant;
use tracing::{debug, info, warn};
use url::Url;

use super::messages::ManagerMessage;
use super::state::{DomainMetadata, ManagerState};
use crate::actors::crawler::worker::messages::WorkerMessage;

pub struct ManagerActor;

impl Actor for ManagerActor {
    type Msg = ManagerMessage;
    type State = ManagerState;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Crawler Manager Shard starting...");
        Ok(ManagerState::new())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ManagerMessage::AddUrls(urls) => {
                for url in urls {
                    if state.seen_urls.insert(url.clone()) {
                        state.frontier.push_back(url);
                    }
                }
            }

            ManagerMessage::UpdateDomainRules { domain, metadata } => {
                state.domain_metadata.insert(domain, metadata);
            }

            ManagerMessage::RequestWork(worker_ref) => {
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
            ManagerMessage::DomainRateLimited { domain, url } => {
                let metadata = state
                    .domain_metadata
                    .entry(domain.clone())
                    .or_insert_with(DomainMetadata::default_unfetched);

                metadata.consecutive_errors += 1;

                let base_delay = std::cmp::max(1, metadata.crawl_delay.as_secs());
                let backoff_secs =
                    std::cmp::min(300, base_delay * 2_u64.pow(metadata.consecutive_errors));

                metadata.backoff_until = Some(Instant::now() + Duration::from_secs(backoff_secs));

                state.frontier.push_front(url);

                tracing::warn!(
                    "Domain {} gave a 429! Backing off for {} seconds.",
                    domain,
                    backoff_secs
                );
            }

            ManagerMessage::CrawlSuccess { domain } => {
                if let Some(metadata) = state.domain_metadata.get_mut(&domain) {
                    metadata.consecutive_errors = 0;
                    metadata.backoff_until = None;
                }
            }
        }
        Ok(())
    }
}
