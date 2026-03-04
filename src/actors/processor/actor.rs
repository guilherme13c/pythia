use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use tracing::{debug, error, info};
use url::Url;

use super::messages::ProcessorMessage;
use super::state::ProcessorState;
use crate::actors::indexer::messages::IndexerMessage;

const CHUNK_SIZE: usize = 200;
const CHUNK_OVERLAP: usize = 50;

pub struct ProcessorActor;

pub fn get_target_shard(url_str: &str, num_shards: usize) -> usize {
    let domain = Url::parse(url_str)
        .map(|u| u.host_str().unwrap_or("unknown").to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    let mut hasher = DefaultHasher::new();
    domain.hash(&mut hasher);
    (hasher.finish() as usize) % num_shards
}

impl ProcessorActor {
    fn get_cache_dir() -> PathBuf {
        let path = env::var("FASTEMBED_CACHE_PATH")
            .unwrap_or_else(|_| "/app/models/fastembed".to_string());
        PathBuf::from(path)
    }

    fn initialize_model() -> TextEmbedding {
        TextEmbedding::try_new(
            InitOptions::new(EmbeddingModel::AllMiniLML6V2).with_cache_dir(Self::get_cache_dir()),
        )
        .unwrap()
    }

    fn clean_text(raw_text: &str) -> String {
        raw_text.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn chunk_text(clean_text: &str, chunk_size: usize, overlap: usize) -> Vec<String> {
        let words: Vec<&str> = clean_text.split(' ').collect();
        let mut chunks = Vec::new();
        let mut i = 0;

        while i < words.len() {
            let end = std::cmp::min(i + chunk_size, words.len());
            chunks.push(words[i..end].join(" "));

            if end == words.len() {
                break;
            }
            i = end - overlap;
        }

        chunks
    }

    fn handle_process_document(&self, state: &mut ProcessorState, url: String, raw_text: String) {
        let clean_text = Self::clean_text(&raw_text);
        let chunks = Self::chunk_text(&clean_text, CHUNK_SIZE, CHUNK_OVERLAP);

        debug!(
            "Processor split document {} into {} chunks. Generating embeddings...",
            url,
            chunks.len()
        );

        match state.embedding_model.embed(chunks.clone(), None) {
            Ok(embeddings) => {
                debug!(
                    "Successfully generated {} vectors for {}",
                    embeddings.len(),
                    url
                );

                let mut indexers = ractor::pg::get_members(&"indexers".to_string());

                if !indexers.is_empty() {
                    indexers.sort_by_key(|m| {
                        m.get_name()
                            .unwrap_or_default()
                            .split('-')
                            .last()
                            .unwrap_or("0")
                            .parse::<usize>()
                            .unwrap_or(0)
                    });

                    let shard_idx = get_target_shard(&url, indexers.len());
                    let indexer_ref: ActorRef<IndexerMessage> = indexers[shard_idx].clone().into();
                    let _ = indexer_ref.cast(IndexerMessage::StoreChunks(url, chunks, embeddings));
                }
            }
            Err(e) => {
                error!("Failed to generate embeddings for {}: {}", url, e);
            }
        }
    }
}

impl Actor for ProcessorActor {
    type Msg = ProcessorMessage;
    type State = ProcessorState;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        ractor::pg::join("processors".to_string(), vec![myself.clone().into()]);

        info!("Processor Actor starting. Loading AI Embedding Model...");

        let embedding_model = Self::initialize_model();

        info!("AI Model loaded successfully!");

        Ok(ProcessorState { embedding_model })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ProcessorMessage::ProcessDocument(url, raw_text) => {
                self.handle_process_document(state, url, raw_text);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_text_removes_messy_whitespace() {
        let raw_html_text = "  This \n\n is   some \t\t very \n messy text.   ";
        let cleaned = ProcessorActor::clean_text(raw_html_text);

        assert_eq!(cleaned, "This is some very messy text.");
    }

    #[test]
    fn test_chunk_text_exact_size() {
        let text = "one two three four five";

        let chunks = ProcessorActor::chunk_text(text, 5, 2);

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], "one two three four five");
    }

    #[test]
    fn test_chunk_text_sliding_window_overlap() {
        let text = "word1 word2 word3 word4 word5 word6 word7";

        let chunks = ProcessorActor::chunk_text(text, 4, 2);

        assert_eq!(chunks.len(), 3, "Should create exactly 3 chunks");
        assert_eq!(chunks[0], "word1 word2 word3 word4");
        assert_eq!(chunks[1], "word3 word4 word5 word6");
        assert_eq!(chunks[2], "word5 word6 word7");
    }

    #[test]
    fn test_chunk_text_smaller_than_chunk_size() {
        let text = "short sentence";

        let chunks = ProcessorActor::chunk_text(text, 10, 5);

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], "short sentence");
    }

    #[test]
    fn test_shard_routing_determinism() {
        let num_shards = 5;

        let shard_a = get_target_shard("https://en.wikipedia.org/wiki/Rust", num_shards);
        let shard_b = get_target_shard("https://en.wikipedia.org/wiki/C++", num_shards);

        assert_eq!(
            shard_a, shard_b,
            "Pages from the same domain must route to the same shard"
        );

        let invalid_url_shard = get_target_shard("not_a_valid_url", num_shards);
        assert!(invalid_url_shard < num_shards);
    }
}
