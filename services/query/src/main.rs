use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    routing::get,
};
use serde::Deserialize;
use shared::models::SearchResult;
use std::env;
use std::sync::Arc;

pub mod logic;
use logic::client::SearchClient;

#[derive(Deserialize)]
struct SearchParams {
    q: String,
    limit: Option<usize>,
}

struct AppState {
    client: SearchClient,
}

#[tokio::main]
async fn main() {
    let processor_url =
        env::var("PROCESSOR_URL").unwrap_or_else(|_| "http://localhost:3001".to_string());
    let indexer_url =
        env::var("INDEXER_URL").unwrap_or_else(|_| "http://localhost:3002".to_string());

    let client = SearchClient::new(processor_url, indexer_url);
    let state = Arc::new(AppState { client });

    let app = Router::new()
        .route("/search", get(search_handler))
        .with_state(state);

    let port = env::var("PORT").unwrap_or_else(|_| "4000".to_string());
    let bind_addr = format!("0.0.0.0:{}", port);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
    println!(
        "🔍 Query API ready on http://localhost:{}/search?q=your+query",
        port
    );
    axum::serve(listener, app).await.unwrap();
}

async fn search_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> Result<Json<Vec<SearchResult>>, (StatusCode, String)> {
    let limit = params.limit.unwrap_or(5);

    if params.q.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Query parameter 'q' cannot be empty".to_string(),
        ));
    }

    match state.client.perform_search(&params.q, limit).await {
        Ok(results) => Ok(Json(results)),
        Err(e) => {
            eprintln!("Search failed: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error during search execution".to_string(),
            ))
        }
    }
}
