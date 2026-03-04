use crate::actors::query::messages::SearchResult;
use fastembed::{TextEmbedding, TextRerank};
use ractor::RpcReplyPort;
use std::collections::HashMap;

pub struct PendingRequest {
    pub reply_port: RpcReplyPort<Vec<SearchResult>>,
    pub original_text: String,
    pub limit: usize,
    pub replies_received: usize,
    pub expected_replies: usize,
    pub all_vec_results: Vec<SearchResult>,
    pub all_fts_results: Vec<SearchResult>,
}

pub struct QueryState {
    pub embedding_model: TextEmbedding,
    pub reranker_model: TextRerank,
    pub indexer_shards: usize,
    pub pending_requests: HashMap<String, PendingRequest>,
}
