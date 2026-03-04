use pythia::actors::crawler::manager::actor::ManagerActor;
use pythia::actors::crawler::manager::messages::ManagerMessage;
use pythia::actors::crawler::worker::actor::WorkerActor;
use pythia::config;
use ractor::Actor;
use ractor_cluster::NodeServer;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

#[tokio::main]
async fn main() {
    let app_config = config::Config::load();

    tracing_subscriber::registry()
        .with(EnvFilter::new(&app_config.log_level))
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .init();

    info!("Starting Pythia Crawler Node...");

    let server = NodeServer::new(
        app_config.cluster_port,
        app_config.cookie.clone(),
        app_config.node_name.clone(),
        app_config.host.clone(),
        None,
        None,
    );

    let (node_ref, _) = Actor::spawn(Some("cluster_node".to_string()), server, ())
        .await
        .expect("Failed to start cluster node");

    if let Some(seed) = &app_config.seed_node
        && !seed.is_empty()
    {
        info!("Connecting to seed node: {}", seed);
        let _ = ractor_cluster::client_connect(
            &node_ref,
            format!("{}:{}", seed, app_config.cluster_port),
        )
        .await;
    }

    let start_idx = app_config.shard_id.unwrap_or(0);
    let end_idx = app_config
        .shard_id
        .map(|i| i + 1)
        .unwrap_or(app_config.crawler_shards);

    let mut manager_cluster = Vec::new();

    for i in start_idx..end_idx {
        let name = format!("manager-{}", i);
        let (manager_ref, _) = Actor::spawn(Some(name), ManagerActor, i).await.unwrap();
        manager_cluster.push(manager_ref.clone());

        for w in 1..=app_config.workers_per_crawler_shard {
            let worker_name = format!("worker-{}-{}", i, w);
            Actor::spawn(
                Some(worker_name),
                WorkerActor,
                (i, app_config.crawler_shards),
            )
            .await
            .unwrap();
        }
    }

    if let Some(0) = app_config.shard_id {
        info!("Shard 0 injecting seed URLs...");
        let seeds = app_config.load_seeds();
        for seed in seeds {
            let domain = Url::parse(&seed).unwrap().host_str().unwrap().to_string();
            let mut hasher = DefaultHasher::new();
            domain.hash(&mut hasher);
            let shard_idx = (hasher.finish() as usize) % app_config.crawler_shards;

            if shard_idx == 0 {
                let _ = manager_cluster[0].cast(ManagerMessage::AddUrls(vec![seed]));
            }
        }
    }

    tokio::signal::ctrl_c().await.unwrap();
    info!("Shutting down Crawler Node...");
}
