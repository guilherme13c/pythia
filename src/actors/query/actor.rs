use arrow::array::{Float32Array, StringArray};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use futures::StreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tracing::{error, info};

use super::messages::{QueryMessage, SearchResult};
use super::state::QueryState;

pub struct QueryActor;

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

        let model = TextEmbedding::try_new(InitOptions::new(EmbeddingModel::AllMiniLML6V2))
            .expect("Failed to initialize Embedding Model in Searcher");

        let db = lancedb::connect("data/pythia-vectors")
            .execute()
            .await
            .expect("Failed to connect to LanceDB");

        let table = db
            .open_table("search_index")
            .execute()
            .await
            .expect("Failed to open table 'search_index'");

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
                let embeddings = match state.embedding_model.embed(vec![text.clone()], None) {
                    Ok(emb) => emb,
                    Err(e) => {
                        error!("Failed to embed query: {}", e);
                        let _ = reply.send(vec![]);
                        return Ok(());
                    }
                };

                let query_vector = &embeddings[0];

                let query_builder = match state.table.query().nearest_to(query_vector.clone()) {
                    Ok(builder) => builder.limit(limit),
                    Err(e) => {
                        tracing::error!("Failed to build query: {}", e);
                        let _ = reply.send(vec![]);
                        return Ok(());
                    }
                };

                let mut stream = match query_builder.execute().await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!("Search failed: {}", e);
                        let _ = reply.send(vec![]);
                        return Ok(());
                    }
                };

                let mut results = Vec::new();

                while let Some(batch_result) = stream.next().await {
                    if let Ok(batch) = batch_result {
                        let url_array = batch
                            .column_by_name("url")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        let text_array = batch
                            .column_by_name("text")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        let dist_array = batch
                            .column_by_name("_distance")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap();

                        for i in 0..batch.num_rows() {
                            results.push(SearchResult {
                                url: url_array.value(i).to_string(),
                                text: text_array.value(i).to_string(),
                                distance: dist_array.value(i),
                            });
                        }
                    }
                }

                let _ = reply.send(results);
            }
        }
        Ok(())
    }
}
