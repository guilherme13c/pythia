use fastembed::TextEmbedding;
use std::sync::{Arc, Mutex};

pub struct ProcessorState {
    pub embedding_model: Arc<Mutex<TextEmbedding>>,
}
