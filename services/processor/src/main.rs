use axum::{
    Router,
    extract::{Json, State},
    routing::post,
};
use processor::communication::{
    consumer::start_document_consumer, publisher::RabbitMqVectorPublisher,
};
use processor::config::ProcessorConfig;
use processor::data::blob_storage::DbBlobStorageReader;
use processor::logic::{
    embedder::Embedder,
    worker::{ProcessorActor, ProcessorState},
};
use ractor::Actor;
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing::info;

#[derive(serde::Deserialize)]
struct EmbedRequest {
    text: String,
}

#[derive(serde::Serialize)]
struct EmbedResponse {
    vector: Vec<f32>,
}

#[derive(serde::Deserialize)]
struct RerankRequest {
    query: String,
    documents: Vec<String>,
}

#[derive(serde::Serialize)]
struct RerankResponse {
    scores: Vec<f32>,
}

async fn handle_embed(
    State(embedder): State<Embedder>,
    Json(req): Json<EmbedRequest>,
) -> Json<EmbedResponse> {
    let chunks = vec![req.text];
    let embedder_clone = embedder.clone();

    let vectors = tokio::task::spawn_blocking(move || {
        embedder_clone.embed_chunks(chunks).unwrap_or_default()
    })
    .await
    .unwrap();

    Json(EmbedResponse {
        vector: vectors.get(0).cloned().unwrap_or_else(|| vec![0.0; 384]),
    })
}

async fn handle_rerank(
    State(embedder): State<Embedder>,
    Json(req): Json<RerankRequest>,
) -> Json<RerankResponse> {
    let embedder_clone = embedder.clone();
    let query = req.query;
    let docs = req.documents;

    let scores = tokio::task::spawn_blocking(move || {
        embedder_clone
            .rerank_documents(&query, docs)
            .unwrap_or_default()
    })
    .await
    .unwrap();

    Json(RerankResponse { scores })
}

#[tokio::main]
async fn main() {
    info!("Bootstrapping Processor Service...");

    let config = ProcessorConfig::load();

    let tracer_provider =
        shared::telemetry::init_telemetry("processor-service", config.otlp_endpoint.clone());

    info!("Loading FastEmbed model (this might take a few seconds)...");
    let embedder = Embedder::new(config.cache_path).expect("Failed to initialize FastEmbed");
    info!("FastEmbed model loaded!");

    let blob_reader =
        Arc::new(DbBlobStorageReader::new(&config.blob_db_path).expect("Failed to init blob DB"));
    let publisher = Arc::new(
        RabbitMqVectorPublisher::new(&config.amqp_addr)
            .await
            .expect("Failed to connect to RabbitMQ"),
    );

    let state = ProcessorState {
        blob_reader,
        publisher,
        embedder: embedder.clone(),
    };

    let (processor_ref, _processor_handle) = Actor::spawn(
        Some("processor-worker-1".to_string()),
        ProcessorActor,
        state,
    )
    .await
    .unwrap();

    start_document_consumer(&config.amqp_addr, processor_ref).await;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let app = Router::new()
        .route("/embed", post(handle_embed))
        .route("/rerank", post(handle_rerank))
        .with_state(embedder.clone())
        .layer(TraceLayer::new_for_http());

    let bind_addr = format!("0.0.0.0:{}", config.port);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
    info!("Processor API listening on port {}", config.port);
    axum::serve(listener, app).await.unwrap();

    if let Some(provider) = tracer_provider {
        let _ = provider.shutdown();
    }
}
