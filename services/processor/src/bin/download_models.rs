use fastembed::{EmbeddingModel, InitOptions, RerankInitOptions, TextEmbedding, TextRerank};
use processor::config::ProcessorConfig;
use std::path::PathBuf;

fn main() {
    println!("Downloading AI Models for the Processor Service...");

    let config = ProcessorConfig::load();
    let cache_dir = PathBuf::from(config.cache_path);

    TextEmbedding::try_new(
        InitOptions::new(EmbeddingModel::AllMiniLML6V2)
            .with_cache_dir(cache_dir.clone())
            .with_show_download_progress(true),
    )
    .expect("Failed to download TextEmbedding model");

    TextRerank::try_new(
        RerankInitOptions::default()
            .with_cache_dir(cache_dir)
            .with_show_download_progress(true),
    )
    .expect("Failed to download TextRerank model");

    println!("Models successfully downloaded and cached!");
}
