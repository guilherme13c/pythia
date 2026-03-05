use ractor::BytesConvertable;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchRequestPayload {
    pub request_id: String,
    pub reply_to: String,
    pub query_vector: Vec<f32>,
    pub fts_query: String,
    pub site_filter: Option<String>,
    pub limit: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum IndexerMessage {
    StoreChunks(
        String,
        Option<String>,
        Option<String>,
        Vec<String>,
        Vec<Vec<f32>>,
    ),
    SearchRequest(SearchRequestPayload),
}

impl BytesConvertable for IndexerMessage {
    fn into_bytes(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
    fn from_bytes(bytes: Vec<u8>) -> Self {
        serde_json::from_slice(&bytes).unwrap()
    }
}
