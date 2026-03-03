use fastembed::TextEmbedding;

pub struct ProcessorState {
    pub embedding_model: TextEmbedding,
    pub num_shards: usize,
}
