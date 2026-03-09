use axum::{
    Json, Router,
    extract::{Query, State},
    routing::get,
};
use serde::Deserialize;
use shared::models::SearchResult;
use std::sync::Arc;

#[derive(Deserialize)]
struct SearchParams {
    q: String,
    limit: Option<usize>,
}

struct AppState {
    http: reqwest::Client,
}

#[tokio::main]
async fn main() {
    let state = Arc::new(AppState {
        http: reqwest::Client::new(),
    });

    let app = Router::new()
        .route("/search", get(search_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:4000").await.unwrap();
    println!("🔍 Query API ready on http://localhost:4000/search?q=your+query");
    axum::serve(listener, app).await.unwrap();
}

async fn search_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> Json<Vec<SearchResult>> {
    let limit = params.limit.unwrap_or(5);

    let embed_resp = state
        .http
        .post("http://localhost:3001/embed")
        .json(&serde_json::json!({ "text": params.q }))
        .send()
        .await
        .unwrap();
    let vector: Vec<f32> = embed_resp.json::<serde_json::Value>().await.unwrap()["vector"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_f64().unwrap() as f32)
        .collect();

    let search_resp = state
        .http
        .post("http://localhost:3002/search")
        .json(&serde_json::json!({ "vector": vector, "limit": limit }))
        .send()
        .await
        .unwrap();

    let results: Vec<SearchResult> = search_resp.json().await.unwrap();
    Json(results)
}
