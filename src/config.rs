use std::env;
use std::fs;
use uuid::Uuid;

pub struct Config {
    pub host: String,
    pub port: u16,
    pub log_level: String,
    pub crawler_shards: usize,
    pub indexer_shards: usize,
    pub workers_per_crawler_shard: usize,
    pub seeds_file: String,
    pub processor_pool_size: usize,
    pub query_pool_size: usize,
    pub bloom_filter_capacity: usize,
    pub bloom_filter_fp_rate: f64,
    pub node_name: String,
    pub cluster_port: u16,
    pub cookie: String,
    pub seed_node: Option<String>,
    pub shard_id: Option<usize>,
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

            crawler_shards: env::var("CRAWLER_SHARDS")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .expect("CRAWLER_SHARDS must be a valid number"),

            indexer_shards: env::var("INDEXER_SHARDS")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .expect("INDEXER_SHARDS must be a valid number"),

            workers_per_crawler_shard: env::var("WORKERS_PER_SHARD")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .expect("WORKERS_PER_CRAWLER_SHARD must be a valid number"),

            seeds_file: env::var("SEEDS_FILE").unwrap_or_else(|_| "seeds.txt".to_string()),

            processor_pool_size: std::env::var("PROCESSOR_POOL_SIZE")
                .unwrap_or_else(|_| "4".to_string())
                .parse()
                .expect("PROCESSOR_POOL_SIZE must be a number"),

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
                        .and_then(|h| h.split('-').last().unwrap_or("").parse().ok())
                }),
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
            crawler_shards: 1,
            indexer_shards: 1,
            workers_per_crawler_shard: 1,
            seeds_file: temp_file.path().to_str().unwrap().to_string(),
            processor_pool_size: 4,
            query_pool_size: 4,
            bloom_filter_capacity: 100000,
            bloom_filter_fp_rate: 0.001,
            node_name: "test-node".to_string(),
            cluster_port: 8000,
            cookie: "test-cookie".to_string(),
            seed_node: None,
            shard_id: None,
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
            env::remove_var("CRAWLER_SHARDS");
            env::remove_var("INDEXER_SHARDS");
            env::remove_var("PROCESSOR_POOL_SIZE");
            env::remove_var("RUN_MODE");
        }

        let config = Config::load();

        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 3000);
        assert_eq!(config.crawler_shards, 3);
        assert_eq!(config.indexer_shards, 3);
        assert_eq!(config.processor_pool_size, 4);
        assert_eq!(config.query_pool_size, 4);
    }
}
