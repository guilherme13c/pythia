use pythia::actors::crawler::manager::actor::ManagerActor;
use pythia::actors::crawler::manager::messages::ManagerMessage;
use pythia::actors::crawler::worker::dynamic_worker::actor::DynamicWorkerActor;
use pythia::actors::crawler::worker::static_worker::actor::WorkerActor;
use pythia::config;
use ractor::Actor;
use ractor_cluster::NodeServer;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

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
        app_config.cluster_host.clone(),
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

    let shard_idx = app_config.shard_id;
    let name = format!("manager-{}", shard_idx);

    let (manager_ref, _) = Actor::spawn(Some(name), ManagerActor, shard_idx)
        .await
        .unwrap();

    for w in 1..=app_config.static_workers_per_crawler_shard {
        let worker_name = format!("worker-{}-{}", shard_idx, w);
        Actor::spawn(Some(worker_name), WorkerActor, shard_idx)
            .await
            .unwrap();
    }

    if app_config.enable_js_rendering {
        info!(
            "JS Rendering Enabled! Spawning {} Dynamic Workers...",
            app_config.dynamic_workers_per_shard
        );
        for w in 1..=app_config.dynamic_workers_per_shard {
            let worker_name = format!("dynamic-worker-{}-{}", shard_idx, w);
            Actor::spawn(Some(worker_name), DynamicWorkerActor, shard_idx)
                .await
                .unwrap();
        }
    }

    if shard_idx == 0 {
        info!("Shard 0 injecting seed URLs...");
        let seeds = app_config.load_seeds();
        let _ = manager_ref.cast(ManagerMessage::AddUrls(seeds));
    }

    tokio::signal::ctrl_c().await.unwrap();
    info!("Shutting down Crawler Node...");
}
