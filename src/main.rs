use crate::actors::crawler::manager::actor::ManagerActor;
use crate::actors::crawler::manager::messages::ManagerMessage;
use crate::actors::crawler::worker::actor::WorkerActor;
use crate::actors::indexer::actor::IndexerActor;
use crate::actors::processor::actor::ProcessorActor;
use crate::actors::query::actor::QueryActor;
use crate::config::RunMode;

use ractor::Actor;
use ractor_cluster::NodeServer;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

pub mod actors;
pub mod api;
pub mod config;

#[tokio::main]
async fn main() {
    let app_config = config::Config::load();

    let console_layer = console_subscriber::ConsoleLayer::builder().spawn();
    let filter_layer = EnvFilter::new(&app_config.log_level);
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);

    tracing_subscriber::registry()
        .with(console_layer)
        .with(filter_layer)
        .with(fmt_layer)
        .init();

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

    if let Some(seed) = &app_config.seed_node {
        info!("Connecting to seed node: {}", seed);
        if let Err(e) = ractor_cluster::client_connect(
            &node_ref,
            format!("{}:{}", seed, app_config.cluster_port), // e.g., "crawler-node-1:8000"
        )
        .await
        {
            tracing::error!("Failed to connect to seed node: {}", e);
        }
    }

    let run_indexer = matches!(app_config.run_mode, RunMode::INDEX | RunMode::FULL);
    let run_processor = matches!(app_config.run_mode, RunMode::PROCESS | RunMode::FULL);
    let run_crawler = matches!(app_config.run_mode, RunMode::CRAWL | RunMode::FULL);
    let run_search = matches!(app_config.run_mode, RunMode::SEARCH | RunMode::FULL);

    info!(
        "Starting Pythia Search Engine... (Mode: {:?})",
        app_config.run_mode
    );

    let mut indexer_cluster = Vec::new();
    let mut processor_pool = Vec::new();
    let mut manager_cluster = Vec::new();
    let mut query_pool = Vec::new();

    if run_indexer {
        for i in 0..app_config.indexer_shards {
            let name = format!("indexer-{}", i);
            let (indexer_ref, _) = Actor::spawn(Some(name), IndexerActor, i)
                .await
                .expect("Failed to start Indexer");
            indexer_cluster.push(indexer_ref);
        }
    }

    if run_processor {
        for i in 0..app_config.processor_pool_size {
            let name = format!("processor-{}", i);
            let (processor_ref, _) =
                Actor::spawn(Some(name), ProcessorActor, app_config.indexer_shards)
                    .await
                    .expect("Failed to start Processor");
            processor_pool.push(processor_ref);
        }
    }

    if run_crawler {
        for i in 0..app_config.crawler_shards {
            let name = format!("manager-{}", i);
            let (manager_ref, _) = Actor::spawn(Some(name), ManagerActor, i)
                .await
                .expect("Failed to start Manager");
            manager_cluster.push(manager_ref);
        }

        for (shard_idx, _) in manager_cluster.iter().enumerate() {
            for w in 1..=app_config.workers_per_crawler_shard {
                let worker_name = format!("worker-{}-{}", shard_idx, w);
                Actor::spawn(
                    Some(worker_name),
                    WorkerActor,
                    (shard_idx, app_config.crawler_shards),
                )
                .await
                .expect("Failed to start Worker");
            }
        }

        info!("Injecting seed URLs from {}...", app_config.seeds_file);
        let seeds = app_config.load_seeds();

        for seed in seeds {
            let domain = Url::parse(&seed).unwrap().host_str().unwrap().to_string();
            let mut hasher = DefaultHasher::new();
            domain.hash(&mut hasher);
            let shard_idx = (hasher.finish() as usize) % app_config.crawler_shards;

            let _ = manager_cluster[shard_idx].cast(ManagerMessage::AddUrls(vec![seed]));
        }
    }

    if run_search {
        for i in 0..app_config.query_pool_size {
            let name = format!("query-{}", i);
            let (query_ref, _) = Actor::spawn(Some(name), QueryActor, app_config.indexer_shards)
                .await
                .expect("Failed to start Searcher");
            query_pool.push(query_ref);
        }

        let bind_addr = format!("{}:{}", app_config.host, app_config.port);
        info!("Starting REST API on http://{}", bind_addr);

        let app = api::build_router(query_pool.clone());
        let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("Failed to start HTTP server");
        });
    }

    info!("Pythia is running! Press Ctrl+C to stop.");
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");

    info!("Shutdown signal received! Stopping actors gracefully...");

    for indexer_ref in indexer_cluster {
        indexer_ref.stop(Some("Ctrl+C Shutdown".to_string()));
    }
    for processor_ref in processor_pool {
        processor_ref.stop(Some("Ctrl+C Shutdown".to_string()));
    }
    for query_ref in query_pool {
        query_ref.stop(Some("Ctrl+C Shutdown".to_string()));
    }
    for manager in manager_cluster {
        manager.stop(Some("Ctrl+C Shutdown".to_string()));
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    info!("All nodes stopped. Exiting Pythia!");

    std::process::exit(0);
}
