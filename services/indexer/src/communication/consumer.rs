use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct VectorPayload {
    pub url: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub chunks: Vec<String>,
    pub embeddings: Vec<Vec<f32>>,
}
