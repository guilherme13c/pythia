use bloomfilter::Bloom;
use rusqlite::Connection;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tokio::time::Instant;

const DEFAULT_CRAWL_DELAY: Duration = Duration::from_secs(2);

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

pub struct ManagerState {
    pub static_frontier: VecDeque<String>,
    pub seen_urls: Bloom<String>,
    pub domain_metadata: HashMap<String, DomainMetadata>,
    pub db: Connection,
}

impl ManagerState {
    pub fn with_db(db_path: &str, capacity: usize, fp_rate: f64) -> Self {
        let db = Connection::open(db_path).expect("Failed to open SQLite DB");

        db.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;",
        )
        .expect("Failed to set SQLite PRAGMAs");

        db.execute(
            "CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                status TEXT NOT NULL
            )",
            [],
        )
        .expect("Failed to create urls table");

        let mut static_frontier = VecDeque::new();
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
                    static_frontier.push_back(url);
                }
            }
        }

        Self {
            static_frontier,
            seen_urls,
            domain_metadata: HashMap::new(),
            db,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_domain_metadata_can_crawl() {
        let mut metadata = DomainMetadata::default_unfetched();

        metadata.disallowed_paths = vec!["/admin".to_string(), "/api/private".to_string()];
        metadata.allowed_paths = vec!["/admin/public".to_string()];

        assert!(metadata.can_crawl("/index.html"));
        assert!(metadata.can_crawl("/about-us"));

        assert!(!metadata.can_crawl("/admin/dashboard"));
        assert!(!metadata.can_crawl("/api/private/users"));

        assert!(metadata.can_crawl("/admin/public/images/logo.png"));
    }

    #[test]
    fn test_manager_state_sqlite_initialization() {
        let mut state = ManagerState::with_db(":memory:", 1000, 0.01);

        let mut stmt = state
            .db
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='urls'")
            .unwrap();
        let mut rows = stmt.query([]).unwrap();

        let table_name: String = rows.next().unwrap().unwrap().get(0).unwrap();
        assert_eq!(table_name, "urls");

        let test_url = "https://example.com".to_string();
        assert!(!state.seen_urls.check(&test_url));
        state.seen_urls.set(&test_url);
        assert!(state.seen_urls.check(&test_url));
    }
}
