use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_tracing::TracingMiddleware;
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
    pub http: ClientWithMiddleware,
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
        let reqwest_client = reqwest::Client::new();
        let tracing_client = ClientBuilder::new(reqwest_client)
            .with(TracingMiddleware::default())
            .build();

        Self {
            processor_url,
            indexer_url,
            http: tracing_client,
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
                    result.cross_encoder_score = *score;
                }
            }

            results.sort_by(|a, b| {
                b.cross_encoder_score
                    .partial_cmp(&a.cross_encoder_score)
                    .unwrap()
            });
        } else {
            results.sort_by(|a, b| a.vector_distance.partial_cmp(&b.vector_distance).unwrap());
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

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;

    #[test]
    fn test_client_initialization() {
        let client = SearchClient::new(
            "http://processor:3001".to_string(),
            "http://indexer:3002".to_string(),
        );
        assert_eq!(client.processor_url, "http://processor:3001");
        assert_eq!(client.indexer_url, "http://indexer:3002");
    }

    #[tokio::test]
    async fn test_fetch_embedding_network_error() {
        let client = SearchClient::new(
            "http://localhost:65000".to_string(),
            "http://localhost:65000".to_string(),
        );

        let result = client.fetch_embedding("test query").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to contact processor"));
    }

    #[tokio::test]
    async fn test_fetch_search_results_handles_indexer_errors() {
        let mut server = Server::new_async().await;

        let _mock = server
            .mock("POST", "/search")
            .with_status(500)
            .create_async()
            .await;

        let client = SearchClient::new("http://localhost:3001".to_string(), server.url());

        let dummy_vector = vec![0.1; 384];
        let result = client.fetch_search_results(dummy_vector, 5).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Indexer API error: 500"));
    }
}
