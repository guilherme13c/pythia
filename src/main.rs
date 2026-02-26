pub mod actors;

use ractor::Actor;
use tracing::info;
use tracing_subscriber::EnvFilter;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use url::Url;

use crate::actors::crawler::manager::actor::ManagerActor;
use crate::actors::crawler::manager::messages::ManagerMessage;
use crate::actors::crawler::worker::actor::WorkerActor;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    let num_shards = 3;
    let workers_per_shard = 5;

    info!("Spinning up {} Manager Shards...", num_shards);
    let mut manager_cluster = Vec::new();

    for i in 0..num_shards {
        let name = format!("manager-{}", i);
        let (manager_ref, _) = Actor::spawn(Some(name), ManagerActor, ())
            .await
            .expect("Failed to start Manager Actor");
        manager_cluster.push(manager_ref);
    }

    for (shard_idx, primary_manager) in manager_cluster.iter().enumerate() {
        for w in 1..=workers_per_shard {
            let worker_name = format!("worker-{}-{}", shard_idx, w);

            Actor::spawn(
                Some(worker_name),
                WorkerActor,
                (manager_cluster.clone(), primary_manager.clone()),
            )
            .await
            .expect("Failed to start Worker");
        }
    }

    info!("Injecting seed URL into the Frontier...");
    let seed_urls = vec![
        "https://quotes.toscrape.com/".to_string(),
        "https://en.wikipedia.org/wiki/Main_Page".to_string(),
        "https://stackexchange.com/".to_string(),
        "https://books.toscrape.com/".to_string(),
    ];

    for seed_url in seed_urls {
        let domain = Url::parse(&seed_url)
            .unwrap()
            .host_str()
            .unwrap()
            .to_string();
        let mut hasher = DefaultHasher::new();
        domain.hash(&mut hasher);
        let shard_idx = (hasher.finish() as usize) % num_shards;

        let _ = manager_cluster[shard_idx].cast(ManagerMessage::AddUrls(vec![seed_url]));
    }

    info!("Engine is running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
    info!("Shutting down...");
}
