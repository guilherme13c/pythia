pub enum IndexerMessage {
    StoreChunks {
        url: String,
        chunks: Vec<String>,
        vectors: Vec<Vec<f32>>,
    },
}
