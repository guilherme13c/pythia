use arrow::array::{Float32Array, StringArray};
use arrow::record_batch::RecordBatch;
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use futures::StreamExt;
use lance_index::scalar::FullTextSearchQuery;
use lancedb::query::{ExecutableQuery, QueryBase};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::collections::HashMap;
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

    fn parse_record_batch(batch: &RecordBatch, is_vector: bool) -> Vec<SearchResult> {
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

        let dist_col = if is_vector {
            batch.column_by_name("_distance")
        } else {
            batch
                .column_by_name("score")
                .or_else(|| batch.column_by_name("_score"))
        };

        let dist_array = dist_col
            .expect("Missing distance/score column")
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Failed to downcast distance/score");

        for i in 0..batch.num_rows() {
            results.push(SearchResult {
                url: url_array.value(i).to_string(),
                text: text_array.value(i).to_string(),
                distance: dist_array.value(i),
            });
        }

        results
    }

    fn compute_rrf(
        mut vec_results: Vec<SearchResult>,
        mut fts_results: Vec<SearchResult>,
        limit: usize,
    ) -> Vec<SearchResult> {
        vec_results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        fts_results.sort_by(|a, b| {
            b.distance
                .partial_cmp(&a.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let k = 60.0;
        let mut rrf_scores: HashMap<(String, String), f32> = HashMap::new();

        for (rank, res) in vec_results.into_iter().enumerate() {
            let key = (res.url, res.text);
            let score = 1.0 / (k + (rank as f32) + 1.0);
            *rrf_scores.entry(key).or_insert(0.0) += score;
        }

        for (rank, res) in fts_results.into_iter().enumerate() {
            let key = (res.url, res.text);
            let score = 1.0 / (k + (rank as f32) + 1.0);
            *rrf_scores.entry(key).or_insert(0.0) += score;
        }

        let mut final_results: Vec<SearchResult> = rrf_scores
            .into_iter()
            .map(|((url, text), combined_score)| SearchResult {
                url,
                text,
                distance: combined_score,
            })
            .collect();

        final_results.sort_by(|a, b| {
            b.distance
                .partial_cmp(&a.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        final_results.truncate(limit);
        final_results
    }

    async fn execute_query(
        state: &mut QueryState,
        parsed_query: &ParsedQuery,
        limit: usize,
    ) -> Result<Vec<SearchResult>, String> {
        let embeddings = state
            .embedding_model
            .embed(vec![parsed_query.original_text.clone()], None)
            .map_err(|e| format!("Failed to embed query: {}", e))?;

        let query_vector = &embeddings[0];

        let mut futures = Vec::new();
        for table in &state.tables {
            let mut vec_query = table
                .query()
                .nearest_to(query_vector.clone())
                .map_err(|e| format!("Failed to build vector query: {}", e))?;

            if let Some(domain) = &parsed_query.site_filter {
                let filter_str = format!("url LIKE '%{}%'", domain);
                vec_query = vec_query.only_if(filter_str);
            }
            let vec_query = vec_query.limit(limit);

            let mut fts_query = table.query().full_text_search(FullTextSearchQuery::new(
                parsed_query.processed_text.clone(),
            ));

            if let Some(domain) = &parsed_query.site_filter {
                let filter_str = format!("url LIKE '%{}%'", domain);
                fts_query = fts_query.only_if(filter_str);
            }
            let fts_query = fts_query.limit(limit);

            futures.push(async move {
                let mut shard_vec_results = Vec::new();
                let mut shard_fts_results = Vec::new();

                if let Ok(mut stream) = vec_query.execute().await {
                    while let Some(Ok(batch)) = stream.next().await {
                        shard_vec_results.extend(Self::parse_record_batch(&batch, true));
                    }
                }

                if !parsed_query.processed_text.is_empty()
                    && let Ok(mut stream) = fts_query.execute().await
                {
                    while let Some(Ok(batch)) = stream.next().await {
                        shard_fts_results.extend(Self::parse_record_batch(&batch, false));
                    }
                }

                Ok::<_, String>((shard_vec_results, shard_fts_results))
            });
        }

        let shard_results = futures::future::join_all(futures).await;

        let mut all_vec_results = Vec::new();
        let mut all_fts_results = Vec::new();

        for res in shard_results.into_iter().flatten() {
            all_vec_results.extend(res.0);
            all_fts_results.extend(res.1);
        }

        Ok(Self::compute_rrf(all_vec_results, all_fts_results, limit))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_parse_record_batch_vector_distance() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("url", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
            Field::new("_distance", DataType::Float32, false),
        ]));

        let url_array = Arc::new(StringArray::from(vec!["https://rust-lang.org"]));
        let text_array = Arc::new(StringArray::from(vec!["Rust is fast"]));
        let dist_array = Arc::new(Float32Array::from(vec![0.15]));

        let batch = RecordBatch::try_new(
            schema,
            vec![url_array as _, text_array as _, dist_array as _],
        )
        .unwrap();

        let results = QueryActor::parse_record_batch(&batch, true);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].url, "https://rust-lang.org");
        assert_eq!(results[0].distance, 0.15);
    }

    #[test]
    fn test_parse_record_batch_fts_score() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("url", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
        ]));

        let url_array = Arc::new(StringArray::from(vec!["https://lancedb.com"]));
        let text_array = Arc::new(StringArray::from(vec!["LanceDB FTS text"]));
        let score_array = Arc::new(Float32Array::from(vec![12.5]));

        let batch = RecordBatch::try_new(
            schema,
            vec![url_array as _, text_array as _, score_array as _],
        )
        .unwrap();

        let results = QueryActor::parse_record_batch(&batch, false);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].url, "https://lancedb.com");
        assert_eq!(results[0].distance, 12.5);
    }

    #[test]
    fn test_compute_rrf_boosts_common_results() {
        let vec_results = vec![
            SearchResult {
                url: "https://doc-a.com".to_string(),
                text: "Text A".to_string(),
                distance: 0.1,
            },
            SearchResult {
                url: "https://doc-b.com".to_string(),
                text: "Text B".to_string(),
                distance: 0.3,
            },
        ];

        let fts_results = vec![
            SearchResult {
                url: "https://doc-b.com".to_string(),
                text: "Text B".to_string(),
                distance: 15.0,
            },
            SearchResult {
                url: "https://doc-c.com".to_string(),
                text: "Text C".to_string(),
                distance: 5.0,
            },
        ];

        let fused = QueryActor::compute_rrf(vec_results, fts_results, 10);

        assert_eq!(fused.len(), 3);

        assert_eq!(
            fused[0].url, "https://doc-b.com",
            "Document B should be boosted to the top by RRF"
        );

        assert!(fused[0].distance > fused[1].distance);
        assert!(fused[1].distance > fused[2].distance);
    }
}
