use axum::{
    Router,
    extract::{Json, State},
    routing::post,
};
use processor::communication::publisher::MockVectorPublisher;
use processor::config::ProcessorConfig;
use processor::data::blob_storage::MockBlobStorageReader;
use processor::logic::embedder::Embedder;
use processor::logic::worker::{ProcessorActor, ProcessorState};
use ractor::Actor;
use std::sync::Arc;
use tracing::info;

#[derive(serde::Deserialize)]
struct EmbedRequest {
    text: String,
}

#[derive(serde::Serialize)]
struct EmbedResponse {
    vector: Vec<f32>,
}

async fn handle_embed(
    State(embedder): State<Embedder>,
    Json(req): Json<EmbedRequest>,
) -> Json<EmbedResponse> {
    let chunks = vec![req.text];
    let vectors = embedder.embed_chunks(chunks).unwrap_or_default();
    Json(EmbedResponse {
        vector: vectors.get(0).cloned().unwrap_or_else(|| vec![0.0; 384]),
    })
}

#[tokio::main]
async fn main() {
    info!("Bootstrapping Processor Service...");

    let config = ProcessorConfig::load();

    info!("Loading FastEmbed model (this might take a few seconds)...");
    let embedder = Embedder::new(config.cache_path).expect("Failed to initialize FastEmbed");
    info!("FastEmbed model loaded!");

    let blob_reader = Arc::new(MockBlobStorageReader);
    let publisher = Arc::new(MockVectorPublisher);

    let state = ProcessorState {
        blob_reader,
        publisher,
        embedder: embedder.clone(),
    };

    let (_processor_ref, _processor_handle) = Actor::spawn(
        Some("processor-worker-1".to_string()),
        ProcessorActor,
        state,
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let app = Router::new()
        .route("/embed", post(handle_embed))
        .with_state(embedder.clone());

    let bind_addr = format!("0.0.0.0:{}", config.port);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
    info!("Processor API listening on port {}", config.port);
    axum::serve(listener, app).await.unwrap();
}
