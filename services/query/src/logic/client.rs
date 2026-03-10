use serde::{Deserialize, Serialize};
use shared::models::SearchResult;

#[derive(Serialize)]
pub struct EmbedRequest {
    pub text: String,
}

#[derive(Deserialize)]
pub struct EmbedResponse {
    pub vector: Vec<f32>,
}

#[derive(Clone)]
pub struct SearchClient {
    pub processor_url: String,
    pub indexer_url: String,
    pub http: reqwest::Client,
}

impl SearchClient {
    pub fn new(processor_url: String, indexer_url: String) -> Self {
        Self {
            processor_url,
            indexer_url,
            http: reqwest::Client::new(),
        }
    }

    pub async fn perform_search(
        &self,
        query_text: &str,
        limit: usize,
    ) -> Result<Vec<SearchResult>, String> {
        let vector = self.fetch_embedding(query_text).await?;
        self.fetch_search_results(vector, limit).await
    }

    async fn fetch_embedding(&self, text: &str) -> Result<Vec<f32>, String> {
        let url = format!("{}/embed", self.processor_url);
        let payload = EmbedRequest {
            text: text.to_string(),
        };

        let resp = self
            .http
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("Failed to contact processor: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Processor API error: {}", resp.status()));
        }

        let embed_resp: EmbedResponse = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse processor response: {}", e))?;

        Ok(embed_resp.vector)
    }

    async fn fetch_search_results(
        &self,
        vector: Vec<f32>,
        limit: usize,
    ) -> Result<Vec<SearchResult>, String> {
        let url = format!("{}/search", self.indexer_url);
        let payload = serde_json::json!({ "vector": vector, "limit": limit });

        let resp = self
            .http
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("Failed to contact indexer: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Indexer API error: {}", resp.status()));
        }

        resp.json::<Vec<SearchResult>>()
            .await
            .map_err(|e| format!("Failed to parse indexer results: {}", e))
    }
}
