use crate::actors::indexer::messages::IndexerMessage;
use fastembed::TextEmbedding;
use ractor::ActorRef;

pub struct ProcessorState {
    pub embedding_model: TextEmbedding,
    pub indexer: ActorRef<IndexerMessage>,
}
