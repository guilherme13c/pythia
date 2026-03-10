use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::communication::publisher::{VectorMessage, VectorPublisher};
use crate::data::blob_storage::BlobStorageReader;
use crate::logic::embedder::{CHUNK_MAX_WORDS, CHUNK_OVERLAP_SENTENCES, Embedder, TextChunker};
use crate::logic::extract;

pub enum ProcessorMessage {
    ProcessDocument {
        url: String,
        blob_id: String,
        mime_type: String,
    },
}

pub struct ProcessorState {
    pub blob_reader: Arc<dyn BlobStorageReader>,
    pub publisher: Arc<dyn VectorPublisher>,
    pub embedder: Embedder,
}

pub struct ProcessorActor;

impl ProcessorActor {
    async fn handle_process_document(
        &self,
        state: &ProcessorState,
        url: String,
        blob_id: String,
        mime_type: String,
    ) {
        info!(
            "⚙️ [Logic Layer] Processing document: {} (Blob: {})",
            url, blob_id
        );

        let Ok(bytes) = state.blob_reader.read_blob(&blob_id).await else {
            error!("Failed to read blob {}: {}", blob_id, url);
            return;
        };

        let Ok(document) = extract::parse_document(&bytes, &mime_type, &url) else {
            error!("Failed to parse document {}", url);
            return;
        };

        let clean_text = TextChunker::clean_text(&document.text);
        let chunks = TextChunker::chunk_text(&clean_text, CHUNK_MAX_WORDS, CHUNK_OVERLAP_SENTENCES);

        if chunks.is_empty() {
            warn!("Document {} resulted in 0 chunks. Skipping.", url);
            return;
        }

        info!("Embedding {} chunks for {}...", chunks.len(), url);
        let Ok(embeddings) = state.embedder.embed_chunks(chunks.clone()) else {
            error!("Failed to generate embeddings for {}", url);
            return;
        };

        let msg = VectorMessage {
            url,
            title: document.title,
            description: document.description,
            chunks,
            embeddings,
        };

        if let Err(e) = state.publisher.publish(msg).await {
            error!("Failed to publish vectors: {}", e);
        }
    }
}

impl Actor for ProcessorActor {
    type Msg = ProcessorMessage;
    type State = ProcessorState;
    type Arguments = ProcessorState;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Processor Actor Started!");
        Ok(state)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ProcessorMessage::ProcessDocument {
                url,
                blob_id,
                mime_type,
            } => {
                self.handle_process_document(state, url, blob_id, mime_type)
                    .await;
            }
        }
        Ok(())
    }
}
