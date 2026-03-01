use crate::config::Config;
use bloomfilter::Bloom;
use rusqlite::Connection;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tokio::time::Instant;

const DEFAULT_CRAWL_DELAY: Duration = Duration::from_secs(2);

pub struct ManagerState {
    pub frontier: VecDeque<String>,
    pub seen_urls: Bloom<String>,
    pub domain_metadata: HashMap<String, DomainMetadata>,
    pub db: Connection,
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

impl Default for ManagerState {
    fn default() -> Self {
        Self::new()
    }
}

impl ManagerState {
    pub fn new() -> Self {
        let config = Config::load();
        Self::with_db(
            "crawler_queue.db",
            config.bloom_filter_capacity,
            config.bloom_filter_fp_rate,
        )
    }

    pub fn in_memory() -> Self {
        Self::with_db(":memory:", 10_000, 0.01)
    }

    pub fn with_db(db_path: &str, capacity: usize, fp_rate: f64) -> Self {
        let db = Connection::open(db_path).expect("Failed to open SQLite DB");

        db.execute(
            "CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                status TEXT NOT NULL
            )",
            [],
        )
        .expect("Failed to create urls table");

        let mut frontier = VecDeque::new();
        let mut seen_urls =
            Bloom::new_for_fp_rate(capacity, fp_rate).expect("Failed to create bloomfilter");

        {
            let mut stmt = db.prepare("SELECT url, status FROM urls").unwrap();
            let url_iter = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap();

            for (url, status) in url_iter.flatten() {
                seen_urls.set(&url);
                if status == "pending" {
                    frontier.push_back(url);
                }
            }
        }

        Self {
            frontier,
            seen_urls,
            domain_metadata: HashMap::new(),
            db,
        }
    }
}
