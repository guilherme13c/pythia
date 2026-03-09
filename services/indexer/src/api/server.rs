use crate::logic::worker::IndexerMessage;
use axum::{
    Router,
    extract::{Json, State},
    routing::post,
};
use ractor::ActorRef;
use serde::Deserialize;
use shared::models::SearchResult;

#[derive(Deserialize)]
pub struct SearchRequest {
    pub vector: Vec<f32>,
    pub limit: usize,
}

pub async fn start_api_server(port: u16, actor_ref: ActorRef<IndexerMessage>) {
    let app = Router::new()
        .route("/search", post(handle_search))
        .with_state(actor_ref);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap();
    println!("📡 Indexer API listening on port {}", port);
    axum::serve(listener, app).await.unwrap();
}

async fn handle_search(
    State(actor): State<ActorRef<IndexerMessage>>,
    Json(payload): Json<SearchRequest>,
) -> Json<Vec<SearchResult>> {
    let (tx, rx) = tokio::sync::oneshot::channel();

    let _ = actor.cast(IndexerMessage::Search {
        query_vector: payload.vector,
        limit: payload.limit,
        reply_to: tx,
    });

    let results = rx.await.unwrap_or_default();
    Json(results)
}
