use crate::data::frontier::{DomainMetadata, ManagerState};
use crate::logic::extract;
use crate::logic::worker::WorkerMessage;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, warn};
use url::Url;

pub enum ManagerMessage {
    AddUrls(Vec<String>),
    RequestWork(String),
    CrawlSuccess(String, String),
    UpdateDomainRules(String, Option<String>),
    DomainRateLimited(String, String),
}

pub struct ManagerActor;

impl ManagerActor {
    fn handle_add_urls(&self, state: &mut ManagerState, urls: Vec<String>) {
        let mut new_urls = Vec::new();

        for url in urls {
            if !state.seen_urls.check(&url) {
                state.seen_urls.set(&url);
                state.static_frontier.push_back(url.clone());
                new_urls.push(url);
            }
        }

        if !new_urls.is_empty() {
            self.persist_new_urls(&mut state.db, new_urls);
        }
    }

    fn persist_new_urls(&self, db: &mut rusqlite::Connection, new_urls: Vec<String>) {
        let Ok(tx) = db.transaction() else {
            return;
        };

        {
            let Ok(mut stmt) = tx.prepare(
                "INSERT INTO urls (url, status) VALUES (?1, 'pending') ON CONFLICT(url) DO NOTHING",
            ) else {
                return;
            };
            for url in new_urls {
                let _ = stmt.execute([&url]);
            }
        }
        let _ = tx.commit();
    }

    fn handle_request_work(&self, state: &mut ManagerState, worker_name: String) {
        let mut next_job = None;

        if let Some(url_str) = state.static_frontier.pop_front() {
            next_job = self.evaluate_url_for_work(state, url_str);
        }

        if let Some(worker_cell) = ractor::registry::where_is(worker_name) {
            let worker_ref: ActorRef<WorkerMessage> = worker_cell.into();
            if let Some(job) = next_job {
                let _ = worker_ref.cast(job);
            }
        }
    }

    fn evaluate_url_for_work(
        &self,
        state: &mut ManagerState,
        url_str: String,
    ) -> Option<WorkerMessage> {
        let parsed_url = Url::parse(&url_str).ok()?;
        let domain = parsed_url.host_str().unwrap_or("unknown").to_string();

        let metadata = state
            .domain_metadata
            .entry(domain.clone())
            .or_insert_with(DomainMetadata::default_unfetched);

        if !metadata.rules_fetched {
            let robots_url = format!("{}://{}/robots.txt", parsed_url.scheme(), domain);
            state.static_frontier.push_front(url_str);
            return Some(WorkerMessage::FetchRobotsTxt(domain, robots_url));
        }

        if let Some(backoff_time) = metadata.backoff_until {
            if Instant::now() < backoff_time {
                state.static_frontier.push_back(url_str);
                return None;
            }
            metadata.backoff_until = None;
        }

        if !metadata.can_crawl(parsed_url.path()) {
            let _ = state
                .db
                .execute("UPDATE urls SET status = 'done' WHERE url = ?1", [&url_str]);
            return None;
        }

        self.check_politeness_and_assign(&mut state.static_frontier, url_str, metadata)
    }

    fn check_politeness_and_assign(
        &self,
        static_frontier: &mut std::collections::VecDeque<String>,
        url_str: String,
        metadata: &mut DomainMetadata,
    ) -> Option<WorkerMessage> {
        let now = Instant::now();
        let last_hit = metadata
            .last_hit
            .unwrap_or_else(|| now.checked_sub(metadata.crawl_delay).unwrap_or(now));

        if now.duration_since(last_hit) >= metadata.crawl_delay {
            metadata.last_hit = Some(now);
            Some(WorkerMessage::Fetch(url_str))
        } else {
            static_frontier.push_back(url_str);
            None
        }
    }

    fn handle_crawl_success(&self, state: &mut ManagerState, domain: String, url: String) {
        if let Some(metadata) = state.domain_metadata.get_mut(&domain) {
            metadata.consecutive_errors = 0;
            metadata.backoff_until = None;
        }
        let _ = state
            .db
            .execute("UPDATE urls SET status = 'done' WHERE url = ?1", [&url]);
    }

    fn handle_update_domain_rules(
        &self,
        state: &mut ManagerState,
        domain: String,
        robots_txt: Option<String>,
    ) {
        let mut metadata = DomainMetadata::default_unfetched();
        metadata.rules_fetched = true;

        if let Some(txt) = robots_txt {
            let (disallow, allow, delay) = extract::parse_robots_txt(&txt);
            metadata.disallowed_paths = disallow;
            metadata.allowed_paths = allow;
            if let Some(d) = delay {
                metadata.crawl_delay = d;
            }
        }
        state.domain_metadata.insert(domain, metadata);
    }

    fn handle_domain_rate_limited(&self, state: &mut ManagerState, domain: String, url: String) {
        let metadata = state
            .domain_metadata
            .entry(domain.clone())
            .or_insert_with(DomainMetadata::default_unfetched);

        metadata.consecutive_errors += 1;

        let base_delay = std::cmp::max(1, metadata.crawl_delay.as_secs());
        let backoff_secs = std::cmp::min(300, base_delay * 2_u64.pow(metadata.consecutive_errors));
        metadata.backoff_until = Some(Instant::now() + Duration::from_secs(backoff_secs));

        state.static_frontier.push_front(url);
        warn!(
            "⚠️ Rate limited on {}. Backing off for {}s",
            domain, backoff_secs
        );
    }
}

impl Actor for ManagerActor {
    type Msg = ManagerMessage;
    type State = ManagerState;
    type Arguments = ManagerState;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("[Logic Layer] Manager Actor started!");
        Ok(state)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ManagerMessage::AddUrls(urls) => self.handle_add_urls(state, urls),
            ManagerMessage::RequestWork(worker_name) => {
                self.handle_request_work(state, worker_name)
            }
            ManagerMessage::CrawlSuccess(domain, url) => {
                self.handle_crawl_success(state, domain, url)
            }
            ManagerMessage::UpdateDomainRules(domain, txt) => {
                self.handle_update_domain_rules(state, domain, txt)
            }
            ManagerMessage::DomainRateLimited(domain, url) => {
                self.handle_domain_rate_limited(state, domain, url)
            }
        }
        Ok(())
    }
}
