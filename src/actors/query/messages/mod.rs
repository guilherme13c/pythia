use ractor::RpcReplyPort;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SearchResult {
    pub url: String,
    pub text: String,
    pub distance: f32,
}

pub enum QueryMessage {
    Query {
        text: String,
        limit: usize,
        reply: RpcReplyPort<Vec<SearchResult>>,
    },
}
