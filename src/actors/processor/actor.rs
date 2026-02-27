use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tracing::{debug, error, info};

use super::messages::ProcessorMessage;
use super::state::ProcessorState;
use crate::actors::indexer::messages::IndexerMessage;

pub struct ProcessorActor;

impl ProcessorActor {
    fn clean_text(raw_text: &str) -> String {
        let cleaned: Vec<&str> = raw_text.split_whitespace().collect();
        cleaned.join(" ")
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
}

impl Actor for ProcessorActor {
    type Msg = ProcessorMessage;
    type State = ProcessorState;
    type Arguments = ActorRef<IndexerMessage>;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        indexer_ref: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Processor Actor starting. Loading AI Embedding Model...");

        let model = TextEmbedding::try_new(
            InitOptions::new(EmbeddingModel::AllMiniLML6V2).with_show_download_progress(true),
        )
        .expect("Failed to initialize the Embedding Model");

        info!("AI Model loaded successfully!");

        Ok(ProcessorState {
            embedding_model: model,
            indexer: indexer_ref,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ProcessorMessage::ProcessDocument { url, raw_text } => {
                let clean_text = Self::clean_text(&raw_text);
                let chunks = Self::chunk_text(&clean_text, 200, 50);

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

                        let _ = state.indexer.cast(IndexerMessage::StoreChunks {
                            url,
                            chunks,
                            vectors: embeddings,
                        });
                    }
                    Err(e) => {
                        error!("Failed to generate embeddings for {}: {}", url, e);
                    }
                }
            }
        }
        Ok(())
    }
}
