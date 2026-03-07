use pythia::actors::query::actor::QueryActor;
use pythia::api;
use pythia::config;
use ractor::Actor;
use ractor_cluster::NodeServer;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    let app_config = config::Config::load();

    tracing_subscriber::registry()
        .with(console_subscriber::ConsoleLayer::builder().spawn())
        .with(EnvFilter::new(&app_config.log_level))
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .init();

    info!("Starting Pythia Seed Node & Search API...");

    let server = NodeServer::new(
        app_config.cluster_port,
        app_config.cookie.clone(),
        app_config.node_name.clone(),
        app_config.cluster_host.clone(),
        None,
        None,
    );

    Actor::spawn(Some("cluster_node".to_string()), server, ())
        .await
        .expect("Failed to start cluster node");

    let mut query_pool = Vec::new();
    for i in 0..app_config.query_pool_size {
        let name = format!("query-{}", i);
        let (query_ref, _) = Actor::spawn(Some(name), QueryActor, ())
            .await
            .expect("Failed to start Searcher");
        query_pool.push(query_ref);
    }

    let bind_addr = format!("{}:{}", app_config.api_host, app_config.api_port);
    info!("Starting REST API on http://{}", bind_addr);

    let app = api::build_router(query_pool.clone());
    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        info!("Ctrl+C received. Gracefully shutting down the HTTP server...");
    })
    .await
    .expect("Failed to start HTTP server");

    info!("Shutting down Seed Node...");
}
