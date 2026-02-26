use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;
use tokio::time::Instant;

const DEFAULT_CRAWL_DELAY: Duration = Duration::from_secs(2);

pub struct ManagerState {
    pub frontier: VecDeque<String>,
    pub seen_urls: HashSet<String>,
    pub domain_metadata: HashMap<String, DomainMetadata>,
}

pub struct DomainMetadata {
    pub last_hit: Option<Instant>,
    pub crawl_delay: Duration,
    pub disallowed_paths: Vec<String>,
    pub allowed_paths: Vec<String>,
    pub rules_fetched: bool,
    pub consecutive_errors: u32,
    pub backoff_until: Option<Instant>,
}

impl DomainMetadata {
    pub fn default_unfetched() -> Self {
        Self {
            last_hit: None,
            crawl_delay: DEFAULT_CRAWL_DELAY,
            disallowed_paths: Vec::new(),
            allowed_paths: Vec::new(),
            rules_fetched: false,
            consecutive_errors: 0,
            backoff_until: None,
        }
    }

    pub fn can_crawl(&self, path: &str) -> bool {
        if self.allowed_paths.iter().any(|p| path.starts_with(p)) {
            return true;
        }
        if self.disallowed_paths.iter().any(|p| path.starts_with(p)) {
            return false;
        }
        true
    }
}

impl ManagerState {
    pub fn new() -> Self {
        Self {
            frontier: VecDeque::new(),
            seen_urls: HashSet::new(),
            domain_metadata: HashMap::new(),
        }
    }
}
