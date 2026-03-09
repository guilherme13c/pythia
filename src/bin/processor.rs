#![recursion_limit = "256"]

use pythia::actors::processor::actor::ProcessorActor;
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

    info!("Starting Pythia Processor Node...");

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
        .unwrap();

    if let Some(seed) = &app_config.seed_node
        && !seed.is_empty()
    {
        let target = format!("{}:{}", seed, app_config.cluster_port);
        tracing::info!("Attempting to connect to seed node: {}", target);

        loop {
            if ractor_cluster::client_connect(&node_ref, target.clone())
                .await
                .is_ok()
            {
                tracing::info!("Successfully connected to cluster!");
                break;
            }
            tracing::warn!("Failed to connect to seed node, retrying in 2 seconds...");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }

    let name = format!("processor-{}", uuid::Uuid::new_v4());
    Actor::spawn(Some(name), ProcessorActor, ()).await.unwrap();

    tokio::signal::ctrl_c().await.unwrap();
}
