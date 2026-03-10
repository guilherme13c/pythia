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

#[derive(Serialize)]
pub struct RerankRequest {
    pub query: String,
    pub documents: Vec<String>,
}

#[derive(Deserialize)]
pub struct RerankResponse {
    pub scores: Vec<f32>,
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

        let mut results = self.fetch_search_results(vector, limit).await?;

        if results.is_empty() {
            return Ok(results);
        }

        let documents: Vec<String> = results.iter().map(|r| r.text.clone()).collect();

        if let Ok(scores) = self.fetch_rerank(query_text, documents).await {
            for (i, result) in results.iter_mut().enumerate() {
                if let Some(score) = scores.get(i) {
                    result.score = *score;
                }
            }

            results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        }

        Ok(results)
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

    async fn fetch_rerank(&self, query: &str, documents: Vec<String>) -> Result<Vec<f32>, String> {
        let url = format!("{}/rerank", self.processor_url);
        let payload = RerankRequest {
            query: query.to_string(),
            documents,
        };

        let resp = self
            .http
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("Failed to contact processor for reranking: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Processor API error on rerank: {}", resp.status()));
        }

        let rerank_resp: RerankResponse = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse rerank response: {}", e))?;

        Ok(rerank_resp.scores)
    }
}
