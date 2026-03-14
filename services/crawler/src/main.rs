use crawler::communication::publisher::RabbitMqPublisher;
use crawler::config::CrawlerConfig;
use crawler::data::{blob_storage::DbBlobStorage, frontier::ManagerState};
use crawler::logic::{
    fetcher::{dynamic_fetcher::DynamicFetcher, static_fetcher::StaticFetcher},
    manager::{ManagerActor, ManagerMessage},
    worker::{WorkerActor, WorkerState},
};
use ractor::Actor;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() {
    info!("Bootstrapping Crawler Service...");

    let config = CrawlerConfig::load();

    let tracer_provider =
        shared::telemetry::init_telemetry("crawler-service", config.otlp_endpoint.clone());

    let blob_storage =
        Arc::new(DbBlobStorage::new(&config.blob_db_path).expect("Failed to init blob DB"));
    let publisher = Arc::new(
        RabbitMqPublisher::new(&config.amqp_addr)
            .await
            .expect("Failed to connect to RabbitMQ"),
    );

    let manager_state = ManagerState::with_db(&config.db_path, 10_000, 0.01);
    let manager_name = format!("manager-{}", config.shard_index);

    let (manager_ref, _) = Actor::spawn(Some(manager_name.clone()), ManagerActor, manager_state)
        .await
        .unwrap();

    let mut worker_id = 1;

    for _ in 0..config.n_dynamic_workers {
        let worker_name = format!("worker-dynamic-{}", worker_id);
        worker_id += 1;

        let worker_state = WorkerState {
            fetcher: Box::new(DynamicFetcher {
                browserless_url: config.browserless_url.clone(),
                http_client: reqwest::Client::new(),
            }),
            blob_storage: blob_storage.clone(),
            publisher: publisher.clone(),
            shard_idx: config.shard_index,
            total_shards: config.total_shards,
        };

        Actor::spawn(Some(worker_name.clone()), WorkerActor, worker_state)
            .await
            .unwrap();

        let _ = manager_ref.cast(ManagerMessage::RequestWork(worker_name));
    }

    for _ in 0..config.n_static_workers {
        let worker_name = format!("worker-static-{}", worker_id);
        worker_id += 1;

        let worker_state = WorkerState {
            fetcher: Box::new(StaticFetcher {
                http_client: reqwest::Client::new(),
            }),
            blob_storage: blob_storage.clone(),
            publisher: publisher.clone(),
            shard_idx: config.shard_index,
            total_shards: config.total_shards,
        };

        Actor::spawn(Some(worker_name.clone()), WorkerActor, worker_state)
            .await
            .unwrap();

        let _ = manager_ref.cast(ManagerMessage::RequestWork(worker_name));
    }

    tokio::signal::ctrl_c().await.unwrap();
    info!("Shutting down Crawler Service...");

    if let Some(provider) = tracer_provider {
        let _ = provider.shutdown();
    }
}
