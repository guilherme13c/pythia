use std::env;
use std::fs;

pub struct Config {
    pub host: String,
    pub port: u16,
    pub log_level: String,
    pub num_shards: usize,
    pub workers_per_shard: usize,
    pub seeds_file: String,
    pub processor_pool_size: usize,
    pub query_pool_size: usize,
}

impl Config {
    pub fn load() -> Self {
        dotenvy::dotenv().ok();

        Self {
            host: env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: env::var("PORT")
                .unwrap_or_else(|_| "3000".to_string())
                .parse()
                .expect("PORT must be a valid number"),
            log_level: env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
            num_shards: env::var("NUM_SHARDS")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .expect("NUM_SHARDS must be a valid number"),
            workers_per_shard: env::var("WORKERS_PER_SHARD")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .expect("WORKERS_PER_SHARD must be a valid number"),
            seeds_file: env::var("SEEDS_FILE").unwrap_or_else(|_| "seeds.txt".to_string()),
            processor_pool_size: std::env::var("PROCESSOR_POOL_SIZE")
                .unwrap_or_else(|_| "4".to_string())
                .parse()
                .expect("PROCESSOR_POOL_SIZE must be a number"),
            query_pool_size: std::env::var("QUERY_POOL_SIZE")
                .unwrap_or_else(|_| "4".to_string())
                .parse()
                .expect("QUERY_POOL_SIZE must be a number"),
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
            num_shards: 1,
            workers_per_shard: 1,
            seeds_file: temp_file.path().to_str().unwrap().to_string(),
            processor_pool_size: 4,
            query_pool_size: 4,
        };

        let seeds = config.load_seeds();

        assert_eq!(seeds.len(), 2);
        assert_eq!(seeds[0], "https://example.com");
        assert_eq!(seeds[1], "https://rust-lang.org");
    }
}
