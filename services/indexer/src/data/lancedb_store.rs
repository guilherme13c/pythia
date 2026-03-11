use arrow_array::{
    Array, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator, StringArray,
    types::Float32Type,
};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use futures::StreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use shared::models::SearchResult;
use std::sync::Arc;

pub const VECTOR_DIMENSIONS: i32 = 384;

pub struct LanceDbStore {
    table: lancedb::Table,
}

impl LanceDbStore {
    pub async fn new(db_uri: &str, table_name: &str) -> Result<Self, String> {
        let conn = lancedb::connect(db_uri)
            .execute()
            .await
            .map_err(|e| format!("Failed to connect to LanceDB: {}", e))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("url", DataType::Utf8, false),
            Field::new("title", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("text", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    VECTOR_DIMENSIONS,
                ),
                false,
            ),
        ]));

        let table = match conn.open_table(table_name).execute().await {
            Ok(t) => t,
            Err(_) => Self::create_empty_table(&conn, table_name, schema.clone()).await?,
        };

        Ok(Self { table })
    }

    async fn create_empty_table(
        conn: &lancedb::Connection,
        table_name: &str,
        schema: Arc<Schema>,
    ) -> Result<lancedb::Table, String> {
        let empty_data: Vec<Result<RecordBatch, ArrowError>> = vec![];
        let empty_batches = Box::new(RecordBatchIterator::new(empty_data, schema));

        conn.create_table(table_name, empty_batches)
            .execute()
            .await
            .map_err(|e| format!("Failed to create table: {}", e))
    }

    pub async fn insert_chunks(
        &self,
        url: &str,
        title: Option<&str>,
        description: Option<&str>,
        chunks: Vec<String>,
        embeddings: Vec<Vec<f32>>,
    ) -> Result<(), String> {
        if chunks.is_empty() || chunks.len() != embeddings.len() {
            return Err("Chunks and embeddings length mismatch".to_string());
        }

        let schema = self.table.schema().await.map_err(|e| e.to_string())?;

        let batch =
            Self::build_record_batch(schema.clone(), url, title, description, chunks, embeddings)?;
        let batches = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema));

        self.table
            .add(batches)
            .execute()
            .await
            .map_err(|e| format!("Failed to insert into LanceDB: {}", e))?;

        Ok(())
    }

    pub fn build_record_batch(
        schema: Arc<Schema>,
        url: &str,
        title: Option<&str>,
        description: Option<&str>,
        chunks: Vec<String>,
        embeddings: Vec<Vec<f32>>,
    ) -> Result<RecordBatch, String> {
        let num_rows = chunks.len();

        let url_array = Arc::new(StringArray::from(vec![url; num_rows]));
        let title_array = Arc::new(StringArray::from(vec![title; num_rows]));
        let desc_array = Arc::new(StringArray::from(vec![description; num_rows]));
        let text_array = Arc::new(StringArray::from(chunks));

        let vector_lists: Vec<Option<Vec<Option<f32>>>> = embeddings
            .into_iter()
            .map(|v| Some(v.into_iter().map(Some).collect()))
            .collect();

        let vector_array = Arc::new(
            FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                vector_lists,
                VECTOR_DIMENSIONS,
            ),
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
        .map_err(|e| format!("Failed to create record batch: {}", e))
    }

    pub async fn search_vector(
        &self,
        vector: Vec<f32>,
        limit: usize,
    ) -> Result<Vec<SearchResult>, String> {
        let query = self
            .table
            .query()
            .nearest_to(vector)
            .map_err(|e| e.to_string())?
            .limit(limit);

        let mut results = Vec::new();
        let mut stream = query.execute().await.map_err(|e| e.to_string())?;

        while let Some(Ok(batch)) = stream.next().await {
            results.extend(Self::parse_record_batch(&batch, true));
        }

        Ok(results)
    }

    fn parse_record_batch(batch: &RecordBatch, is_vector: bool) -> Vec<SearchResult> {
        let url_array = Self::get_string_array(batch, "url");
        let text_array = Self::get_string_array(batch, "text");
        let title_array = Self::get_string_array(batch, "title");

        let desc_array = Self::get_string_array(batch, "description");

        let score_col = if is_vector { "_distance" } else { "_score" };
        let score_array = batch
            .column_by_name(score_col)
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();

        (0..batch.num_rows())
            .map(|i| {
                let text = text_array.value(i).to_string();

                let snippet = if text.chars().count() > 160 {
                    let mut s: String = text.chars().take(160).collect();
                    s.push_str("...");
                    s
                } else {
                    text.clone()
                };

                SearchResult {
                    url: url_array.value(i).to_string(),
                    text,
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
                    score: score_array.value(i),
                    snippet,
                }
            })
            .collect()
    }

    fn get_string_array<'a>(batch: &'a RecordBatch, col_name: &str) -> &'a StringArray {
        batch
            .column_by_name(col_name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }
}

// ... (Leave the existing `mod tests` block unchanged here) ...

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn get_test_db_path() -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let path = format!("target/test-data/indexer-{}", id);
        let _ = fs::create_dir_all(&path);
        path
    }

    #[tokio::test]
    async fn test_lancedb_initialization_and_insertion() {
        let db_path = get_test_db_path();
        let table_name = "test_table";

        let store = LanceDbStore::new(&db_path, table_name)
            .await
            .expect("Failed to initialize LanceDB store");

        let url = "https://rust-lang.org";
        let title = Some("Rust Programming Language");
        let description =
            Some("A language empowering everyone to build reliable and efficient software.");
        let chunks = vec![
            "Rust is a multi-paradigm, general-purpose programming language.".to_string(),
            "It is designed for performance and safety, especially safe concurrency.".to_string(),
        ];

        let embeddings = vec![
            vec![0.1f32; VECTOR_DIMENSIONS as usize],
            vec![0.2f32; VECTOR_DIMENSIONS as usize],
        ];

        let result = store
            .insert_chunks(url, title, description, chunks, embeddings)
            .await;
        assert!(result.is_ok(), "Insertion failed: {:?}", result.err());

        let _ = fs::remove_dir_all(db_path);
    }

    #[tokio::test]
    async fn test_insert_mismatch_error() {
        let db_path = get_test_db_path();
        let store = LanceDbStore::new(&db_path, "error_table").await.unwrap();

        let result = store
            .insert_chunks(
                "url",
                None,
                None,
                vec!["chunk1".to_string()],
                vec![vec![0.0; 384], vec![0.0; 384]],
            )
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Chunks and embeddings length mismatch");

        let _ = fs::remove_dir_all(db_path);
    }

    #[tokio::test]
    async fn test_search_and_parse_snippets() {
        let db_path = get_test_db_path();
        let table_name = "snippet_test_table";

        let store = LanceDbStore::new(&db_path, table_name)
            .await
            .expect("Failed to initialize LanceDB store");

        let url = "https://example.com";
        let title = Some("Test Title");
        let description = Some("This is a test description that should be preserved.");

        let long_text = "This is a very long text chunk that intentionally exceeds one hundred and sixty characters to test if the snippet generation logic correctly truncates it and adds an ellipsis.";

        let chunks = vec![long_text.to_string()];
        let embeddings = vec![vec![0.5f32; VECTOR_DIMENSIONS as usize]];

        store
            .insert_chunks(url, title, description, chunks, embeddings.clone())
            .await
            .unwrap();

        let results = store.search_vector(embeddings[0].clone(), 1).await.unwrap();

        assert_eq!(results.len(), 1);
        let result = &results[0];

        assert_eq!(
            result.description.as_deref(),
            Some("This is a test description that should be preserved.")
        );

        assert!(result.snippet.ends_with("..."));
        assert_eq!(result.snippet.chars().count(), 163);

        assert_eq!(result.text, long_text);

        let _ = fs::remove_dir_all(db_path);
    }
}
