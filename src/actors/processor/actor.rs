use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};
use url::Url;

use super::messages::ProcessorMessage;
use super::state::ProcessorState;
use crate::actors::indexer::messages::IndexerMessage;

const CHUNK_MAX_WORDS: usize = 200;
const CHUNK_OVERLAP_SENTENCES: usize = 2;

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

    pub fn clean_text(raw_text: &str) -> String {
        raw_text.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    pub fn separate_sentences(clean_text: &str) -> Vec<String> {
        let mut sentences = Vec::new();
        let mut current_sentence = String::new();

        for c in clean_text.chars() {
            current_sentence.push(c);
            if c == '.' || c == '!' || c == '?' {
                sentences.push(current_sentence.trim().to_string());
                current_sentence.clear();
            }
        }
        if !current_sentence.trim().is_empty() {
            sentences.push(current_sentence.trim().to_string());
        }

        sentences
    }

    pub fn chunk_text(clean_text: &str, max_words: usize, overlap_sentences: usize) -> Vec<String> {
        let sentences = Self::separate_sentences(clean_text);
        if sentences.is_empty() {
            return Vec::new();
        }

        let mut chunks = Vec::new();
        let mut i = 0;

        while i < sentences.len() {
            let mut chunk_words = 0;
            let mut chunk_sentences = Vec::new();
            let mut j = i;

            while j < sentences.len() {
                let sentence = &sentences[j];
                let sentence_words = sentence.split_whitespace().count();

                if chunk_words + sentence_words > max_words && !chunk_sentences.is_empty() {
                    break;
                }

                chunk_sentences.push(sentence.as_str());
                chunk_words += sentence_words;
                j += 1;
            }

            chunks.push(chunk_sentences.join(" "));
            if j == sentences.len() {
                break;
            }
            i = std::cmp::max(i + 1, j.saturating_sub(overlap_sentences));
        }

        chunks
    }

    async fn handle_process_document(
        &self,
        state: &mut ProcessorState,
        url: String,
        raw_text: String,
        title: Option<String>,
        description: Option<String>,
    ) {
        let clean_text = Self::clean_text(&raw_text);
        let chunks = Self::chunk_text(&clean_text, CHUNK_MAX_WORDS, CHUNK_OVERLAP_SENTENCES);

        if chunks.is_empty() {
            return;
        }

        let model = state.embedding_model.clone();
        let chunks_clone = chunks.clone();
        let url_clone = url.clone();

        let embedding_task = tokio::task::spawn_blocking(move || {
            let mut model_guard = model.lock().unwrap();
            model_guard.embed(chunks_clone, None)
        })
        .await;

        match embedding_task {
            Ok(Ok(embeddings)) => {
                let mut indexers = ractor::pg::get_members(&"indexers".to_string());

                if !indexers.is_empty() {
                    indexers.sort_by_key(|m| {
                        m.get_name()
                            .unwrap_or_default()
                            .split('-')
                            .next_back()
                            .unwrap_or("0")
                            .parse::<usize>()
                            .unwrap_or(0)
                    });

                    let shard_idx = get_target_shard(&url_clone, indexers.len());
                    let indexer_ref: ActorRef<IndexerMessage> = indexers[shard_idx].clone().into();
                    let _ = indexer_ref.cast(IndexerMessage::StoreChunks(
                        url_clone,
                        title,
                        description,
                        chunks,
                        embeddings,
                    ));
                }
            }
            Ok(Err(e)) => error!("Failed to generate embeddings for {}: {:?}", url, e),
            Err(e) => error!("Tokio blocking task failed for {}: {:?}", url, e),
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

        Ok(ProcessorState {
            embedding_model: Arc::new(std::sync::Mutex::new(Self::initialize_model())),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ProcessorMessage::ProcessDocument(url, raw_text, title, description) => {
                self.handle_process_document(state, url, raw_text, title, description)
                    .await;
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
    fn test_separate_sentences() {
        let text = "Hello world! How are you? I am fine. This has no punctuation";

        let sentences = ProcessorActor::separate_sentences(text);

        assert_eq!(sentences.len(), 4);
        assert_eq!(sentences[0], "Hello world!");
        assert_eq!(sentences[1], "How are you?");
        assert_eq!(sentences[2], "I am fine.");
        assert_eq!(sentences[3], "This has no punctuation");
    }

    #[test]
    fn test_chunk_text_semantic_boundaries() {
        let text = "One. Two. Three. Four. Five.";

        let chunks = ProcessorActor::chunk_text(text, 5, 1);

        assert_eq!(chunks.len(), 1, "Should group short sentences together");
        assert_eq!(chunks[0], "One. Two. Three. Four. Five.");
    }

    #[test]
    fn test_chunk_text_sliding_window_sentence_overlap() {
        let text = "Sentence one is here. Sentence two is short. Sentence three. Sentence four is longer. Sentence five.";

        let chunks = ProcessorActor::chunk_text(text, 8, 1);

        assert_eq!(chunks.len(), 3);

        assert_eq!(chunks[0], "Sentence one is here. Sentence two is short.");

        assert_eq!(chunks[1], "Sentence two is short. Sentence three.");

        assert_eq!(
            chunks[2],
            "Sentence three. Sentence four is longer. Sentence five."
        );
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
    }
}
