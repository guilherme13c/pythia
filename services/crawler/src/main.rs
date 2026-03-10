use ractor::Actor;
use std::sync::Arc;
use tokio::time::Duration;
use url::Url;

pub mod communication;
pub mod data;
pub mod logic;

use communication::publisher::MockPublisher;
use data::blob_storage::MockBlobStorage;
use data::frontier::ManagerState;
use logic::manager::{ManagerActor, ManagerMessage};

use headless_chrome::{Browser, LaunchOptions};
use logic::worker::{WorkerActor, WorkerState, WorkerType, get_shard_index};

#[tokio::main]
async fn main() {
    println!("🚀 Bootstrapping Crawler Service...");

    let blob_storage = Arc::new(MockBlobStorage);
    let publisher = Arc::new(MockPublisher);

    let total_shards = 3;
    let mut manager_refs = Vec::new();

    for i in 0..total_shards {
        let manager_state = ManagerState::with_db(":memory:", 10_000, 0.01);
        let manager_name = format!("manager-{}", i);

        let (manager_ref, _) = Actor::spawn(Some(manager_name), ManagerActor, manager_state)
            .await
            .unwrap();

        manager_refs.push(manager_ref);
    }

    let num_workers = 3;
    for i in 1..=num_workers {
        let worker_name = format!("worker-{}", i);
        let shard_idx = i % total_shards;

        let worker_type = if i == 1 {
            let browser = Browser::new(
                LaunchOptions::default_builder()
                    .headless(true)
                    .build()
                    .expect("Failed to build LaunchOptions"),
            )
            .expect("Failed to launch headless chrome");
            WorkerType::Dynamic(Arc::new(browser))
        } else {
            WorkerType::Static
        };

        let worker_state = WorkerState {
            http_client: reqwest::Client::new(),
            blob_storage: blob_storage.clone(),
            publisher: publisher.clone(),
            worker_type,
            shard_idx,
            total_shards,
        };

        Actor::spawn(Some(worker_name.clone()), WorkerActor, worker_state)
            .await
            .unwrap();

        let _ = manager_refs[shard_idx].cast(ManagerMessage::RequestWork(worker_name));
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    let seed_urls = vec![
        "https://www.rust-lang.org/".to_string(),
        "https://tokio.rs/".to_string(),
    ];

    for url in seed_urls {
        if let Ok(parsed) = Url::parse(&url) {
            let domain = parsed.host_str().unwrap_or("");
            let shard = get_shard_index(domain, total_shards);
            let _ = manager_refs[shard].cast(ManagerMessage::AddUrls(vec![url]));
        }
    }

    tokio::signal::ctrl_c().await.unwrap();
    println!("Shutting down Crawler Service...");
}
