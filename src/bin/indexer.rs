#![recursion_limit = "256"]

use pythia::actors::indexer::actor::IndexerActor;
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

    info!("Starting Pythia Indexer Node...");

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

    let shard_idx = app_config.shard_id;
    let name = format!("indexer-{}", shard_idx);
    Actor::spawn(Some(name), IndexerActor, shard_idx)
        .await
        .unwrap();

    tokio::signal::ctrl_c().await.unwrap();
}

#[cfg(test)]
mod tests {
    use arrow::array::{Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use pythia::actors::indexer::actor::IndexerActor;
    use std::sync::Arc;

    #[test]
    fn test_parse_record_batch_vector_distance() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("url", DataType::Utf8, false),
            Field::new("title", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("text", DataType::Utf8, false),
            Field::new("_distance", DataType::Float32, false),
        ]));

        let url_array = Arc::new(StringArray::from(vec!["https://rust-lang.org"]));
        let title_array = Arc::new(StringArray::from(vec![Some("Rust Programming Language")]));
        let desc_array = Arc::new(StringArray::from(vec![Some(
            "A language empowering everyone...",
        )]));
        let text_array = Arc::new(StringArray::from(vec!["Rust is fast"]));
        let dist_array = Arc::new(Float32Array::from(vec![0.15]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                url_array as _,
                title_array as _,
                desc_array as _,
                text_array as _,
                dist_array as _,
            ],
        )
        .unwrap();

        let results = IndexerActor::parse_record_batch(&batch, true);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].url, "https://rust-lang.org");
        assert_eq!(
            results[0].title.as_deref(),
            Some("Rust Programming Language")
        );
        assert_eq!(results[0].distance, 0.15);
    }

    #[test]
    fn test_parse_record_batch_fts_score() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("url", DataType::Utf8, false),
            Field::new("title", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("text", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
        ]));

        let url_array = Arc::new(StringArray::from(vec!["https://lancedb.com"]));
        let title_array = Arc::new(StringArray::from(vec![Option::<&str>::None]));
        let desc_array = Arc::new(StringArray::from(vec![Option::<&str>::None]));
        let text_array = Arc::new(StringArray::from(vec!["LanceDB FTS text"]));
        let score_array = Arc::new(Float32Array::from(vec![12.5]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                url_array as _,
                title_array as _,
                desc_array as _,
                text_array as _,
                score_array as _,
            ],
        )
        .unwrap();

        let results = IndexerActor::parse_record_batch(&batch, false);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].url, "https://lancedb.com");
        assert_eq!(results[0].title, None);
        assert_eq!(results[0].distance, 12.5);
    }
}
