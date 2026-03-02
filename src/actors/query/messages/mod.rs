use ractor::RpcReplyPort;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SearchResult {
    pub url: String,
    pub text: String,
    pub distance: f32,
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub semantic_text: String,
    pub site_filter: Option<String>,
}

impl ParsedQuery {
    pub fn parse(raw_query: &str) -> Self {
        let mut semantic_parts = Vec::new();
        let mut site_filter = None;

        for token in raw_query.split_whitespace() {
            if let Some(site) = token.strip_prefix("site:") {
                site_filter = Some(site.to_string());
            } else {
                semantic_parts.push(token);
            }
        }

        Self {
            semantic_text: semantic_parts.join(" "),
            site_filter,
        }
    }
}

pub enum QueryMessage {
    Query {
        parsed_query: ParsedQuery,
        limit: usize,
        reply: RpcReplyPort<Vec<SearchResult>>,
    },
}
