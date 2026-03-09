use serde::{Deserialize, Serialize};
use shared::models::SearchResult;

#[derive(Serialize)]
struct EmbedRequest {
    text: String,
}

#[derive(Deserialize)]
struct EmbedResponse {
    vector: Vec<f32>,
}

pub struct SearchClient {
    pub processor_url: String,
    pub indexer_url: String,
    pub http: reqwest::Client,
}

impl SearchClient {
    pub async fn perform_search(
        &self,
        query_text: &str,
        limit: usize,
    ) -> Result<Vec<SearchResult>, String> {
        let embed_resp = self
            .http
            .post(format!("{}/embed", self.processor_url))
            .json(&EmbedRequest {
                text: query_text.to_string(),
            })
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let vector = embed_resp
            .json::<EmbedResponse>()
            .await
            .map_err(|e| e.to_string())?
            .vector;

        let search_resp = self
            .http
            .post(format!("{}/search", self.indexer_url))
            .json(&serde_json::json!({ "vector": vector, "limit": limit }))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        search_resp
            .json::<Vec<SearchResult>>()
            .await
            .map_err(|e| e.to_string())
    }
}
