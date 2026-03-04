use ractor::BytesConvertable;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum IndexerMessage {
    StoreChunks(String, Vec<String>, Vec<Vec<f32>>),
    SearchRequest {
        request_id: String,
        reply_to: String,
        query_vector: Vec<f32>,
        fts_query: String,
        site_filter: Option<String>,
        limit: usize,
    },
}

impl BytesConvertable for IndexerMessage {
    fn into_bytes(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
    fn from_bytes(bytes: Vec<u8>) -> Self {
        serde_json::from_slice(&bytes).unwrap()
    }
}
