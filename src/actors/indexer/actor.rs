use arrow::datatypes::{Field, Schema, DataType, Float32Type};
use arrow::array::{FixedSizeListArray, StringArray };
use arrow::record_batch::{ RecordBatchIterator, RecordBatch };
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;

use super::messages::IndexerMessage;
use super::state::IndexerState;

pub struct IndexerActor;

impl Actor for IndexerActor {
    type Msg = IndexerMessage;
    type State = IndexerState;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        tracing::info!("Starting Vector DB Indexer...");

        let db = lancedb::connect("data/pythia-vectors")
            .execute()
            .await
            .expect("Failed to connect to LanceDB");

        let schema = Arc::new(Schema::new(vec![
            Field::new("url", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 384),
                false,
            ),
        ]));

        let table_name = "search_index";
        let table = match db.open_table(table_name).execute().await {
            Ok(t) => {
                tracing::info!("Found existing vector table.");
                t
            }
            Err(_) => {
                tracing::info!("Creating new vector table from scratch...");
                db.create_empty_table(table_name, schema.clone())
                    .execute()
                    .await
                    .expect("Failed to create table")
            }
        };

        Ok(IndexerState { table })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            IndexerMessage::StoreChunks {
                url,
                chunks,
                vectors,
            } => {
                let num_rows = chunks.len();
                if num_rows == 0 {
                    return Ok(());
                }

                let url_array = Arc::new(StringArray::from(vec![url.clone(); num_rows]));
                let text_array = Arc::new(StringArray::from(chunks));

                let flat_vectors: Vec<Option<f32>> =
                    vectors.into_iter().flatten().map(Some).collect();

                let vector_array = Arc::new(FixedSizeListArray::from_iter_primitive::<
                    Float32Type,
                    _,
                    _,
                >(vec![Some(flat_vectors); 1], 384));

                let schema = state.table.schema().await.unwrap();
                let batch =
                    RecordBatch::try_new(schema.clone(), vec![url_array, text_array, vector_array])
                        .expect("Failed to build Arrow RecordBatch");

                let batches = RecordBatchIterator::new(vec![Ok(batch)], schema);
                match state.table.add(Box::new(batches)).execute().await {
                    Ok(_) => {
                        tracing::info!("Successfully saved {} vectors to DB for {}", num_rows, url)
                    }
                    Err(e) => tracing::error!("Database error inserting {}: {}", url, e),
                }
            }
        }
        Ok(())
    }
}
