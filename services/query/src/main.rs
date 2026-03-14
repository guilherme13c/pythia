use axum::{
    Json, Router,
    body::Body,
    extract::{Query, State},
    http::StatusCode,
    response::Response,
    routing::get,
};
use query::config::QueryConfig;
use query::logic::client::SearchClient;
use serde::Deserialize;
use shared::models::SearchResult;
use std::sync::Arc;
use std::time::Duration;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

#[derive(Deserialize)]
struct SearchParams {
    q: String,
    limit: Option<usize>,
}

struct AppState {
    client: SearchClient,
}

#[tokio::main]
async fn main() {
    let config = QueryConfig::load();

    let tracer_provider =
        shared::telemetry::init_telemetry("query-service", config.otlp_endpoint.clone());

    let client = SearchClient::new(config.processor_url, config.indexer_url);
    let state = Arc::new(AppState { client });

    let app = Router::new()
        .route("/search", get(search_handler))
        .route("/debug/pprof/profile", get(profile))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let bind_addr = format!("0.0.0.0:{}", config.port);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
    info!(
        "Query API ready on http://localhost:{}/search?q=your+query",
        config.port
    );
    axum::serve(listener, app).await.unwrap();

    if let Some(provider) = tracer_provider {
        let _ = provider.shutdown();
    }
}

async fn search_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> Result<Json<Vec<SearchResult>>, (StatusCode, String)> {
    let limit = params.limit.unwrap_or(5);

    if params.q.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Query parameter 'q' cannot be empty".to_string(),
        ));
    }

    match state.client.perform_search(&params.q, limit).await {
        Ok(results) => Ok(Json(results)),
        Err(e) => {
            error!("Search failed: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error during search execution".to_string(),
            ))
        }
    }
}

async fn profile() -> Response<Body> {
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(100)
        .build()
        .unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;

    if let Ok(report) = guard.report().build() {
        let mut body = Vec::new();
        report.flamegraph(&mut body).unwrap();
        return Response::builder()
            .header("Content-Type", "image/svg+xml")
            .body(Body::from(body))
            .unwrap();
    }
    Response::builder().status(500).body(Body::empty()).unwrap()
}
