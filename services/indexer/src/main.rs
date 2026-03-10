use ractor::Actor;
use std::sync::Arc;
use tracing::info;

pub mod api;
pub mod communication;
pub mod config;
pub mod data;
pub mod logic;

use api::server::start_api_server;
use config::IndexerConfig;
use data::lancedb_store::LanceDbStore;
use logic::worker::{IndexerActor, IndexerState};

#[tokio::main]
async fn main() {
    info!("Bootstrapping Indexer Service...");

    let config = IndexerConfig::load();

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

    start_api_server(config.port, actor_ref).await;
}
