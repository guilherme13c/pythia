use arrow::array::{FixedSizeListArray, StringArray};
use arrow::datatypes::{DataType, Field, Float32Type, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;
use tracing::{error, info};

use super::messages::IndexerMessage;
use super::state::IndexerState;

pub struct IndexerActor;

impl IndexerActor {
    fn get_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("url", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 384),
                false,
            ),
        ]))
    }

    async fn initialize_database() -> lancedb::Table {
        let db = lancedb::connect("data/pythia-vectors")
            .execute()
            .await
            .expect("Failed to connect to LanceDB");

        let schema = Self::get_schema();
        let table_name = "search_index";

        match db.open_table(table_name).execute().await {
            Ok(t) => {
                info!("Found existing vector table.");
                t
            }
            Err(_) => {
                info!("Creating new vector table from scratch...");
                db.create_empty_table(table_name, schema)
                    .execute()
                    .await
                    .expect("Failed to create table")
            }
        }
    }

    fn build_record_batch(
        schema: Arc<Schema>,
        url: &str,
        chunks: Vec<String>,
        vectors: Vec<Vec<f32>>,
    ) -> Result<RecordBatch, arrow::error::ArrowError> {
        let num_rows = chunks.len();

        let url_array = Arc::new(StringArray::from(vec![url; num_rows]));
        let text_array = Arc::new(StringArray::from(chunks));

        let vector_lists: Vec<Option<Vec<Option<f32>>>> = vectors
            .into_iter()
            .map(|v| Some(v.into_iter().map(Some).collect()))
            .collect();

        let vector_array = Arc::new(FixedSizeListArray::from_iter_primitive::<
            Float32Type,
            _,
            _,
        >(vector_lists, 384));

        RecordBatch::try_new(schema, vec![url_array, text_array, vector_array])
    }

    async fn handle_store_chunks(
        &self,
        state: &mut IndexerState,
        url: String,
        chunks: Vec<String>,
        vectors: Vec<Vec<f32>>,
    ) {
        let num_rows = chunks.len();
        if num_rows == 0 {
            return;
        }

        let schema = state.table.schema().await.unwrap();

        let batch = match Self::build_record_batch(schema.clone(), &url, chunks, vectors) {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to build Arrow RecordBatch for {}: {}", url, e);
                return;
            }
        };

        let batches = RecordBatchIterator::new(vec![Ok(batch)], schema);

        match state.table.add(Box::new(batches)).execute().await {
            Ok(_) => {
                info!("Successfully saved {} vectors to DB for {}", num_rows, url)
            }
            Err(e) => error!("Database error inserting {}: {}", url, e),
        }
    }
}

impl Actor for IndexerActor {
    type Msg = IndexerMessage;
    type State = IndexerState;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Starting Vector DB Indexer...");

        let table = Self::initialize_database().await;

        Ok(IndexerState { table })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            IndexerMessage::StoreChunks { url, chunks, vectors } => {
                self.handle_store_chunks(state, url, chunks, vectors).await;
            }
        }
        Ok(())
    }
}
