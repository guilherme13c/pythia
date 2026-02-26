use ractor::Actor;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::actors::crawler::manager::actor::ManagerActor;
use crate::actors::crawler::manager::messages::ManagerMessage;
use crate::actors::crawler::worker::actor::WorkerActor;
use crate::actors::indexer::actor::IndexerActor;
use crate::actors::processor::actor::ProcessorActor;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use url::Url;

pub mod actors;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    info!("Starting Pythia Search Engine...");

    let (indexer_ref, _) = Actor::spawn(Some("indexer".to_string()), IndexerActor, ())
        .await
        .expect("Failed to start Indexer");

    let (processor_ref, _) =
        Actor::spawn(Some("processor".to_string()), ProcessorActor, indexer_ref)
            .await
            .expect("Failed to start Processor");

    let num_shards = 3;
    let workers_per_shard = 5;
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

    info!("Pythia is running! Press Ctrl+C to stop.");
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
}
