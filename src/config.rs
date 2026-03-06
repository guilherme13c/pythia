use std::env;
use std::fs;
use uuid::Uuid;

pub struct Config {
    pub host: String,
    pub port: u16,
    pub log_level: String,
    pub static_workers_per_crawler_shard: usize,
    pub enable_js_rendering: bool,
    pub dynamic_workers_per_shard: usize,
    pub seeds_file: String,
    pub query_pool_size: usize,
    pub bloom_filter_capacity: usize,
    pub bloom_filter_fp_rate: f64,
    pub node_name: String,
    pub cluster_port: u16,
    pub cookie: String,
    pub seed_node: Option<String>,
    pub shard_id: usize,
}

impl Config {
    pub fn load() -> Self {
        #[cfg(not(test))]
        if dotenvy::dotenv().is_err() {
            dotenvy::from_filename("local.env").ok();
        }

        Self {
            host: env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),

            port: env::var("PORT")
                .unwrap_or_else(|_| "3000".to_string())
                .parse()
                .expect("PORT must be a valid number"),

            log_level: env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),

            static_workers_per_crawler_shard: env::var("STATIC_WORKERS_PER_SHARD")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .expect("STATIC_WORKERS_PER_SHARD must be a valid number"),

            enable_js_rendering: std::env::var("ENABLE_JS_RENDERING")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),

            dynamic_workers_per_shard: std::env::var("DYNAMIC_WORKERS_PER_SHARD")
                .unwrap_or_else(|_| "1".to_string())
                .parse()
                .unwrap_or(1),

            seeds_file: env::var("SEEDS_FILE").unwrap_or_else(|_| "seeds.txt".to_string()),

            query_pool_size: std::env::var("QUERY_POOL_SIZE")
                .unwrap_or_else(|_| "4".to_string())
                .parse()
                .expect("QUERY_POOL_SIZE must be a number"),

            bloom_filter_capacity: env::var("BLOOM_FILTER_CAPACITY")
                .unwrap_or_else(|_| "10000000".to_string())
                .parse()
                .expect("BLOOM_FILTER_CAPACITY must be a number"),

            bloom_filter_fp_rate: env::var("BLOOM_FILTER_FP_RATE")
                .unwrap_or_else(|_| "0.001".to_string())
                .parse()
                .expect("BLOOM_FILTER_FP_RATE must be a float"),

            node_name: std::env::var("NODE_NAME").unwrap_or_else(|_| Uuid::new_v4().to_string()),

            cluster_port: std::env::var("CLUSTER_PORT")
                .unwrap_or_else(|_| "8000".to_string())
                .parse()
                .expect("CLUSTER_PORT must be a valid number"),

            cookie: std::env::var("CLUSTER_COOKIE")
                .unwrap_or_else(|_| "pythia_secret_cookie".to_string()),

            seed_node: std::env::var("SEED_NODE").ok(),

            shard_id: std::env::var("SHARD_ID")
                .ok()
                .and_then(|v| v.parse().ok())
                .or_else(|| {
                    std::env::var("HOSTNAME")
                        .ok()
                        .and_then(|h| h.split('-').next_back().unwrap_or("").parse().ok())
                })
                .unwrap_or(0),
        }
    }

    pub fn load_seeds(&self) -> Vec<String> {
        let content = fs::read_to_string(&self.seeds_file)
            .unwrap_or_else(|_| panic!("Failed to read seeds file at path: {}", self.seeds_file));

        content
            .lines()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty() && !s.starts_with('#'))
            .map(|s| s.to_string())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_seeds_ignores_comments_and_empty_lines() {
        let mut temp_file = NamedTempFile::new().unwrap();

        writeln!(temp_file, "# This is a comment").unwrap();
        writeln!(temp_file, "https://example.com").unwrap();
        writeln!(temp_file, "   ").unwrap();
        writeln!(temp_file, "https://rust-lang.org").unwrap();

        let config = Config {
            host: "127.0.0.1".to_string(),
            port: 8080,
            log_level: "info".to_string(),
            static_workers_per_crawler_shard: 1,
            enable_js_rendering: false,
            dynamic_workers_per_shard: 1,
            seeds_file: temp_file.path().to_str().unwrap().to_string(),
            query_pool_size: 4,
            bloom_filter_capacity: 100000,
            bloom_filter_fp_rate: 0.001,
            node_name: "test-node".to_string(),
            cluster_port: 8000,
            cookie: "test-cookie".to_string(),
            seed_node: None,
            shard_id: 0,
        };

        let seeds = config.load_seeds();

        assert_eq!(seeds.len(), 2);
        assert_eq!(seeds[0], "https://example.com");
        assert_eq!(seeds[1], "https://rust-lang.org");
    }

    #[test]
    fn test_config_defaults() {
        unsafe {
            env::remove_var("HOST");
            env::remove_var("PORT");
            env::remove_var("QUERY_POOL_SIZE");
        }

        let config = Config::load();

        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 3000);
        assert_eq!(config.query_pool_size, 4);
    }
}
