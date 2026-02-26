use fastembed::TextEmbedding;
use lancedb::Table;

pub struct QueryState {
    pub embedding_model: TextEmbedding,
    pub table: Table,
}
