use crate::actors::crawler::manager::actor::ManagerActor;
use crate::actors::crawler::manager::messages::ManagerMessage;
use crate::actors::crawler::worker::actor::WorkerActor;
use crate::actors::indexer::actor::IndexerActor;
use crate::actors::processor::actor::ProcessorActor;
use crate::actors::query::actor::QueryActor;
use ractor::Actor;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::info;
use tracing_subscriber::EnvFilter;
use url::Url;

pub mod actors;
pub mod api;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    info!("Starting Pythia Search Engine...");

    let (indexer_ref, _) = Actor::spawn(Some("indexer".to_string()), IndexerActor, ())
        .await
        .expect("Failed to start Indexer");

    let (processor_ref, _) = Actor::spawn(
        Some("processor".to_string()),
        ProcessorActor,
        indexer_ref.clone(),
    )
    .await
    .expect("Failed to start Processor");

    let num_shards = 3;
    let workers_per_shard = 3;
    let mut manager_cluster = Vec::new();

    for i in 0..num_shards {
        let name = format!("manager-{}", i);
        let (manager_ref, _) = Actor::spawn(Some(name), ManagerActor, ())
            .await
            .expect("Failed to start Manager");
        manager_cluster.push(manager_ref);
    }

    for (shard_idx, primary_manager) in manager_cluster.iter().enumerate() {
        for w in 1..=workers_per_shard {
            let worker_name = format!("worker-{}-{}", shard_idx, w);
            Actor::spawn(
                Some(worker_name),
                WorkerActor,
                (
                    manager_cluster.clone(),
                    primary_manager.clone(),
                    processor_ref.clone(),
                ),
            )
            .await
            .expect("Failed to start Worker");
        }
    }

    info!("Injecting seed URLs...");
    let seeds = vec![
        "https://quotes.toscrape.com/".to_string(),
        "https://books.toscrape.com/".to_string(),
        "https://en.wikipedia.com/".to_string(),
    ];

    for seed in seeds {
        let domain = Url::parse(&seed).unwrap().host_str().unwrap().to_string();
        let mut hasher = DefaultHasher::new();
        domain.hash(&mut hasher);
        let shard_idx = (hasher.finish() as usize) % num_shards;

        let _ = manager_cluster[shard_idx].cast(ManagerMessage::AddUrls(vec![seed]));
    }

    let (query_ref, _) = Actor::spawn(Some("query".to_string()), QueryActor, ())
        .await
        .expect("Failed to start Searcher");

    info!("Starting REST API on http://0.0.0.0:3000");
    let app = api::build_router(query_ref.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("Failed to start HTTP server");
    });

    info!("Pythia is running! Press Ctrl+C to stop.");
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");

    info!("Shutdown signal received! Stopping actors gracefully...");

    processor_ref.stop(Some("Ctrl+C Shutdown".to_string()));
    indexer_ref.stop(Some("Ctrl+C Shutdown".to_string()));
    query_ref.stop(Some("Ctrl+C Shutdown".to_string()));

    for manager in manager_cluster {
        manager.stop(Some("Ctrl+C Shutdown".to_string()));
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    info!("All nodes stopped. Exiting Pythia!");

    std::process::exit(0);
}
