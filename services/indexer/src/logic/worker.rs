use crate::data::lancedb_store::LanceDbStore;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use shared::models::SearchResult;
use std::sync::Arc;

pub enum IndexerMessage {
    Store {
        url: String,
        title: Option<String>,
        description: Option<String>,
        chunks: Vec<String>,
        embeddings: Vec<Vec<f32>>,
    },
    Search {
        query_vector: Vec<f32>,
        limit: usize,
        reply_to: tokio::sync::oneshot::Sender<Vec<SearchResult>>,
    },
}

pub struct IndexerState {
    pub store: Arc<LanceDbStore>,
}

pub struct IndexerActor;

impl IndexerActor {
    async fn handle_store(
        &self,
        state: &IndexerState,
        url: String,
        title: Option<String>,
        description: Option<String>,
        chunks: Vec<String>,
        embeddings: Vec<Vec<f32>>,
    ) {
        let chunk_count = chunks.len();
        let result = state
            .store
            .insert_chunks(
                &url,
                title.as_deref(),
                description.as_deref(),
                chunks,
                embeddings,
            )
            .await;

        match result {
            Ok(_) => println!("✅ Indexed {} chunks for {}", chunk_count, url),
            Err(e) => eprintln!("❌ Database insertion error for {}: {}", url, e),
        }
    }

    async fn handle_search(
        &self,
        state: &IndexerState,
        query_vector: Vec<f32>,
        limit: usize,
        reply_to: tokio::sync::oneshot::Sender<Vec<SearchResult>>,
    ) {
        let results = state
            .store
            .search_vector(query_vector, limit)
            .await
            .unwrap_or_default();

        let _ = reply_to.send(results);
    }
}

impl Actor for IndexerActor {
    type Msg = IndexerMessage;
    type State = IndexerState;
    type Arguments = IndexerState;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(state)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            IndexerMessage::Store {
                url,
                title,
                description,
                chunks,
                embeddings,
            } => {
                self.handle_store(state, url, title, description, chunks, embeddings)
                    .await;
            }
            IndexerMessage::Search {
                query_vector,
                limit,
                reply_to,
            } => {
                self.handle_search(state, query_vector, limit, reply_to)
                    .await;
            }
        }
        Ok(())
    }
}

// ... (Leave the existing `mod tests` block unchanged here) ...

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::lancedb_store::VECTOR_DIMENSIONS;
    use std::fs;

    fn get_test_db_path() -> String {
        let id = uuid::Uuid::new_v4().to_string();
        format!("target/test-data/actor-{}", id)
    }

    #[tokio::test]
    async fn test_indexer_actor_store_and_search_flow() {
        let db_path = get_test_db_path();
        let store = Arc::new(LanceDbStore::new(&db_path, "actor_test").await.unwrap());
        let state = IndexerState { store };

        let (actor_ref, handle) = Actor::spawn(None, IndexerActor, state).await.unwrap();

        let url = "https://example.com".to_string();
        let chunks = vec!["This is a test chunk".to_string()];
        let embeddings = vec![vec![0.5f32; VECTOR_DIMENSIONS as usize]];

        actor_ref
            .cast(IndexerMessage::Store {
                url: url.clone(),
                title: Some("Test".to_string()),
                description: None,
                chunks,
                embeddings,
            })
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let (tx, rx) = tokio::sync::oneshot::channel::<Vec<SearchResult>>();
        actor_ref
            .cast(IndexerMessage::Search {
                query_vector: vec![0.5f32; VECTOR_DIMENSIONS as usize],
                limit: 1,
                reply_to: tx,
            })
            .unwrap();

        let results = rx.await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].url, url);
        assert_eq!(results[0].text, "This is a test chunk");

        actor_ref.stop(None);
        handle.await.unwrap();
        let _ = fs::remove_dir_all(db_path);
    }
}
