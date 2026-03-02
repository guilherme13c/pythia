use fastembed::{TextEmbedding, TextRerank};
use lancedb::Table;

pub struct QueryState {
    pub embedding_model: TextEmbedding,
    pub reranker_model: TextRerank,
    pub tables: Vec<Table>,
}
