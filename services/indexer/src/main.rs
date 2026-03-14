use indexer::api::server::start_api_server;
use indexer::communication::consumer::start_vector_consumer;
use indexer::config::IndexerConfig;
use indexer::data::lancedb_store::LanceDbStore;
use indexer::logic::worker::{IndexerActor, IndexerState};
use ractor::Actor;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() {
    info!("Bootstrapping Indexer Service...");

    let config = IndexerConfig::load();

    let tracer_provider =
        shared::telemetry::init_telemetry("indexer-service", config.otlp_endpoint.clone());

    info!("Connecting to LanceDB at {}...", config.lancedb_uri);
    let store = LanceDbStore::new(&config.lancedb_uri, &config.lancedb_table)
        .await
        .expect("Failed to initialize LanceDB");
    info!("Database connected!");

    let state = IndexerState {
        store: Arc::new(store),
    };

    let (actor_ref, _handle) = Actor::spawn(Some("indexer-actor".to_string()), IndexerActor, state)
        .await
        .expect("Failed to spawn IndexerActor");

    start_vector_consumer(&config.amqp_addr, actor_ref.clone()).await;

    start_api_server(config.port, actor_ref).await;

    if let Some(provider) = tracer_provider {
        let _ = provider.shutdown();
    }
}
