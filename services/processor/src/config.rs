use std::env;

#[derive(Debug, Clone)]
pub struct ProcessorConfig {
    pub port: u16,
    pub cache_path: String,
}

impl ProcessorConfig {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();

        Self {
            port: env::var("PROCESSOR_PORT")
                .unwrap_or_else(|_| "3001".to_string())
                .parse()
                .unwrap_or(3001),
            cache_path: env::var("FASTEMBED_CACHE_PATH")
                .unwrap_or_else(|_| ".local_models/fastembed".to_string()),
        }
    }
}
