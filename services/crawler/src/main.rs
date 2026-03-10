use crawler::communication::publisher::MockPublisher;
use crawler::config::CrawlerConfig;
use crawler::data::blob_storage::MockBlobStorage;
use crawler::data::frontier::ManagerState;
use crawler::logic::manager::{ManagerActor, ManagerMessage};
use crawler::logic::worker::{WorkerActor, WorkerState, WorkerType, get_shard_index};
use headless_chrome::{Browser, LaunchOptions};
use ractor::Actor;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::info;
use url::Url;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    info!("🚀 Bootstrapping Crawler Service...");

    let config = CrawlerConfig::load();

    let blob_storage = Arc::new(MockBlobStorage);
    let publisher = Arc::new(MockPublisher);

    let mut manager_refs = Vec::new();

    for i in 0..config.total_shards {
        let manager_state = ManagerState::with_db(&config.db_path, 10_000, 0.01);
        let manager_name = format!("manager-{}", i);

        let (manager_ref, _) = Actor::spawn(Some(manager_name), ManagerActor, manager_state)
            .await
            .unwrap();

        manager_refs.push(manager_ref);
    }

    for i in 1..=config.num_workers {
        let worker_name = format!("worker-{}", i);
        let shard_idx = i % config.total_shards;

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
            total_shards: config.total_shards,
        };

        Actor::spawn(Some(worker_name.clone()), WorkerActor, worker_state)
            .await
            .unwrap();

        let _ = manager_refs[shard_idx].cast(ManagerMessage::RequestWork(worker_name));
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    for url in config.seed_urls {
        if let Ok(parsed) = Url::parse(&url) {
            let domain = parsed.host_str().unwrap_or("");
            let shard = get_shard_index(domain, config.total_shards);
            let _ = manager_refs[shard].cast(ManagerMessage::AddUrls(vec![url]));
        }
    }

    tokio::signal::ctrl_c().await.unwrap();
    info!("Shutting down Crawler Service...");
}
