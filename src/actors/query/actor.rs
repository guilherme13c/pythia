use arrow::array::{Float32Array, StringArray};
use arrow::record_batch::RecordBatch;
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use futures::StreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tracing::{error, info};

use super::messages::{ParsedQuery, QueryMessage, SearchResult};
use super::state::QueryState;

pub struct QueryActor;

impl QueryActor {
    async fn initialize_databases(num_shards: usize) -> Vec<lancedb::Table> {
        let mut tables = Vec::new();

        for i in 0..num_shards {
            let db_path = format!("data/pythia-vectors-{}", i);
            let db = lancedb::connect(&db_path)
                .execute()
                .await
                .expect("Failed to connect to LanceDB");

            let mut retries = 0;
            let table = loop {
                match db.open_table("search_index").execute().await {
                    Ok(t) => break t,
                    Err(e) => {
                        if retries > 10 {
                            panic!("Failed to open table for shard {}: {}", i, e);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        retries += 1;
                    }
                }
            };
            tables.push(table);
        }
        tables
    }

    fn initialize_model() -> TextEmbedding {
        TextEmbedding::try_new(InitOptions::new(EmbeddingModel::AllMiniLML6V2))
            .expect("Failed to initialize Embedding Model in Searcher")
    }

    fn parse_record_batch(batch: &RecordBatch) -> Vec<SearchResult> {
        let mut results = Vec::with_capacity(batch.num_rows());

        let url_array = batch
            .column_by_name("url")
            .expect("Missing 'url' column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast 'url'");

        let text_array = batch
            .column_by_name("text")
            .expect("Missing 'text' column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast 'text'");

        let dist_array = batch
            .column_by_name("_distance")
            .expect("Missing '_distance' column")
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Failed to downcast '_distance'");

        for i in 0..batch.num_rows() {
            results.push(SearchResult {
                url: url_array.value(i).to_string(),
                text: text_array.value(i).to_string(),
                distance: dist_array.value(i),
            });
        }

        results
    }

    async fn execute_query(
        state: &mut QueryState,
        parsed_query: &ParsedQuery,
        limit: usize,
    ) -> Result<Vec<SearchResult>, String> {
        let embeddings = state
            .embedding_model
            .embed(vec![parsed_query.processed_text.clone()], None)
            .map_err(|e| format!("Failed to embed query: {}", e))?;

        let query_vector = &embeddings[0];

        let mut futures = Vec::new();
        for table in &state.tables {
            let mut query_builder = table
                .query()
                .nearest_to(query_vector.clone())
                .map_err(|e| format!("Failed to build query: {}", e))?;

            if let Some(domain) = &parsed_query.site_filter {
                let filter_str = format!("url LIKE '%{}%'", domain);
                query_builder = query_builder.only_if(filter_str);
            }

            let query_builder = query_builder.limit(limit);

            futures.push(async move {
                let mut stream = query_builder.execute().await.map_err(|e| e.to_string())?;
                let mut shard_results = Vec::new();
                while let Some(batch_result) = stream.next().await {
                    if let Ok(batch) = batch_result {
                        shard_results.extend(Self::parse_record_batch(&batch));
                    }
                }
                Ok::<Vec<SearchResult>, String>(shard_results)
            });
        }

        let shard_results = futures::future::join_all(futures).await;

        let mut all_results = Vec::new();
        for res in shard_results {
            if let Ok(results) = res {
                all_results.extend(results);
            }
        }

        all_results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        all_results.truncate(limit);

        Ok(all_results)
    }
}

impl Actor for QueryActor {
    type Msg = QueryMessage;
    type State = QueryState;
    type Arguments = usize;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        num_shards: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Starting Searcher Actor...");

        let model = Self::initialize_model();
        let tables = Self::initialize_databases(num_shards).await;

        Ok(QueryState {
            embedding_model: model,
            tables,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            QueryMessage::Query {
                parsed_query,
                limit,
                reply,
            } => {
                let results = match Self::execute_query(state, &parsed_query, limit).await {
                    Ok(res) => res,
                    Err(e) => {
                        error!("{}", e);
                        vec![]
                    }
                };

                let _ = reply.send(results);
            }
        }
        Ok(())
    }
}
