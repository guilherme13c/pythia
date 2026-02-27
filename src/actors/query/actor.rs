use arrow::array::{Float32Array, StringArray};
use arrow::record_batch::RecordBatch;
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use futures::StreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tracing::{error, info};

use super::messages::{QueryMessage, SearchResult};
use super::state::QueryState;

pub struct QueryActor;

impl QueryActor {
    async fn initialize_database() -> lancedb::Table {
        let db = lancedb::connect("data/pythia-vectors")
            .execute()
            .await
            .expect("Failed to connect to LanceDB");

        db.open_table("search_index")
            .execute()
            .await
            .expect("Failed to open table 'search_index'")
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
        text: &str,
        limit: usize,
    ) -> Result<Vec<SearchResult>, String> {
        let embeddings = state
            .embedding_model
            .embed(vec![text.to_string()], None)
            .map_err(|e| format!("Failed to embed query: {}", e))?;

        let query_vector = &embeddings[0];

        let query_builder = state
            .table
            .query()
            .nearest_to(query_vector.clone())
            .map_err(|e| format!("Failed to build query: {}", e))?
            .limit(limit);

        let mut stream = query_builder
            .execute()
            .await
            .map_err(|e| format!("Search failed: {}", e))?;

        let mut results = Vec::new();

        while let Some(batch_result) = stream.next().await {
            match batch_result {
                Ok(batch) => {
                    results.extend(Self::parse_record_batch(&batch));
                }
                Err(e) => {
                    error!("Error reading batch from stream: {}", e);
                }
            }
        }

        Ok(results)
    }
}

impl Actor for QueryActor {
    type Msg = QueryMessage;
    type State = QueryState;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Starting Searcher Actor...");

        let model = Self::initialize_model();
        let table = Self::initialize_database().await;

        Ok(QueryState {
            embedding_model: model,
            table,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            QueryMessage::Query { text, limit, reply } => {
                let results = match Self::execute_query(state, &text, limit).await {
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
