use arrow::array::Array;
use arrow::array::{FixedSizeListArray, Float32Array, StringArray};
use arrow::datatypes::{DataType, Field, Float32Type, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use futures::StreamExt;
use lance_index::scalar::FullTextSearchQuery;
use lancedb::index::Index;
use lancedb::index::scalar::FtsIndexBuilder;
use lancedb::query::{ExecutableQuery, QueryBase};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;
use tracing::{error, info};

use super::messages::{IndexerMessage, SearchRequestPayload};
use super::state::IndexerState;
use crate::actors::query::messages::{QueryMessage, QueryNetworkMessage, SearchResult};

pub struct IndexerActor;

impl IndexerActor {
    fn get_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("url", DataType::Utf8, false),
            Field::new("title", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("text", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 384),
                false,
            ),
        ]))
    }

    async fn initialize_database(shard_idx: usize) -> lancedb::Table {
        let db_path = format!("data/pythia-vectors-{}", shard_idx);
        let db = lancedb::connect(&db_path)
            .execute()
            .await
            .expect("Failed to connect to LanceDB");

        let schema = Self::get_schema();
        let table_name = "search_index";

        match db.open_table(table_name).execute().await {
            Ok(t) => {
                info!("Found existing vector table for shard {}.", shard_idx);
                t
            }
            Err(_) => {
                info!("Creating new vector table for shard {}...", shard_idx);
                let table = db
                    .create_empty_table(table_name, schema)
                    .execute()
                    .await
                    .expect("Failed to create table");

                table
                    .create_index(&["text"], Index::FTS(FtsIndexBuilder::default()))
                    .execute()
                    .await
                    .expect("Failed to create FTS index");

                table
            }
        }
    }

    fn build_record_batch(
        schema: Arc<Schema>,
        url: &str,
        title: Option<String>,
        description: Option<String>,
        chunks: Vec<String>,
        vectors: Vec<Vec<f32>>,
    ) -> Result<RecordBatch, arrow::error::ArrowError> {
        let num_rows = chunks.len();

        let url_array = Arc::new(StringArray::from(vec![url; num_rows]));
        let title_array = Arc::new(StringArray::from(vec![title.as_deref(); num_rows]));
        let desc_array = Arc::new(StringArray::from(vec![description.as_deref(); num_rows]));
        let text_array = Arc::new(StringArray::from(chunks));

        let vector_lists: Vec<Option<Vec<Option<f32>>>> = vectors
            .into_iter()
            .map(|v| Some(v.into_iter().map(Some).collect()))
            .collect();

        let vector_array = Arc::new(
            FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(vector_lists, 384),
        );

        RecordBatch::try_new(
            schema,
            vec![
                url_array as _,
                title_array as _,
                desc_array as _,
                text_array as _,
                vector_array as _,
            ],
        )
    }

    async fn handle_store_chunks(
        &self,
        state: &mut IndexerState,
        url: String,
        title: Option<String>,
        description: Option<String>,
        chunks: Vec<String>,
        vectors: Vec<Vec<f32>>,
    ) {
        let num_rows = chunks.len();
        if num_rows == 0 {
            return;
        }

        let schema = state.table.schema().await.unwrap();

        let batch = match Self::build_record_batch(
            schema.clone(),
            &url,
            title,
            description,
            chunks,
            vectors,
        ) {
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

    pub fn parse_record_batch(batch: &RecordBatch, is_vector: bool) -> Vec<SearchResult> {
        let mut results = Vec::with_capacity(batch.num_rows());
        let url_array = batch
            .column_by_name("url")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let title_array = batch
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let desc_array = batch
            .column_by_name("description")
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

        let dist_col = if is_vector {
            batch.column_by_name("_distance")
        } else {
            batch
                .column_by_name("score")
                .or_else(|| batch.column_by_name("_score"))
        };
        let dist_array = dist_col
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            results.push(SearchResult {
                url: url_array.value(i).to_string(),
                text: text_array.value(i).to_string(),
                title: if title_array.is_null(i) {
                    None
                } else {
                    Some(title_array.value(i).to_string())
                },
                description: if desc_array.is_null(i) {
                    None
                } else {
                    Some(desc_array.value(i).to_string())
                },
                distance: dist_array.value(i),
                snippet: String::new(),
            });
        }
        results
    }

    async fn handle_search_request(&self, state: &mut IndexerState, req: SearchRequestPayload) {
        let mut shard_vec_results = Vec::new();
        let mut shard_fts_results = Vec::new();

        let mut vec_query = state
            .table
            .query()
            .nearest_to(req.query_vector.clone())
            .unwrap();

        if let Some(domain) = &req.site_filter {
            vec_query = vec_query.only_if(format!("url LIKE '%{}%'", domain));
        }

        if let Ok(mut stream) = vec_query.limit(req.limit).execute().await {
            while let Some(Ok(batch)) = stream.next().await {
                shard_vec_results.extend(Self::parse_record_batch(&batch, true));
            }
        }

        if !req.fts_query.is_empty() {
            let mut fts_q = state
                .table
                .query()
                .full_text_search(FullTextSearchQuery::new(req.fts_query));

            if let Some(domain) = &req.site_filter {
                fts_q = fts_q.only_if(format!("url LIKE '%{}%'", domain));
            }

            if let Ok(mut stream) = fts_q.limit(req.limit).execute().await {
                while let Some(Ok(batch)) = stream.next().await {
                    shard_fts_results.extend(Self::parse_record_batch(&batch, false));
                }
            }
        }

        if let Some(cell) = ractor::pg::get_members(&req.reply_to).first() {
            let query_actor: ActorRef<QueryMessage> = cell.clone().into();
            let msg = QueryNetworkMessage::IndexerReply {
                request_id: req.request_id,
                shard_vec_results,
                shard_fts_results,
            };
            let _ = query_actor.cast(QueryMessage::Network(msg));
        } else {
            error!(
                "Could not find query actor on the network to reply to: {}",
                req.reply_to
            );
        }
    }
}

impl Actor for IndexerActor {
    type Msg = IndexerMessage;
    type State = IndexerState;
    type Arguments = usize;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        shard_idx: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        ractor::pg::join("indexers".to_string(), vec![myself.clone().into()]);

        let table = Self::initialize_database(shard_idx).await;

        Ok(IndexerState { table })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!("received message: {:?}", message);
        match message {
            IndexerMessage::StoreChunks(url, title, description, chunks, vectors) => {
                self.handle_store_chunks(state, url, title, description, chunks, vectors)
                    .await;
            }

            IndexerMessage::SearchRequest(req) => {
                self.handle_search_request(state, req).await;
            }
        }
        Ok(())
    }
}
