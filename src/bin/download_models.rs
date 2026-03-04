use fastembed::{EmbeddingModel, InitOptions, RerankInitOptions, TextEmbedding, TextRerank};
use std::env;
use std::path::PathBuf;

fn main() {
    println!("Downloading AI Models during Docker build...");

    let cache_path_str =
        env::var("FASTEMBED_CACHE_PATH").unwrap_or_else(|_| "/app/models/fastembed".to_string());

    let cache_dir = PathBuf::from(cache_path_str);

    TextEmbedding::try_new(
        InitOptions::new(EmbeddingModel::AllMiniLML6V2)
            .with_cache_dir(cache_dir.clone())
            .with_show_download_progress(true),
    )
    .expect("Failed to download TextEmbedding model");

    TextRerank::try_new(
        RerankInitOptions::default()
            .with_cache_dir(cache_dir.clone())
            .with_show_download_progress(true),
    )
    .expect("Failed to download TextRerank model");

    println!("Models successfully downloaded and cached!");
}
