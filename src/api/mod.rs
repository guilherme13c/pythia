use axum::{Router, routing::get};
use ractor::ActorRef;

use crate::actors::query::messages::QueryMessage;

pub mod handlers;
pub mod models;

pub fn build_router(query_pool: Vec<ActorRef<QueryMessage>>) -> Router {
    Router::new()
        .route("/search", get(handlers::search_handler))
        .with_state(query_pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::query::messages::SearchResult;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use ractor::{Actor, ActorProcessingErr};
    use serde_json::Value;
    use tower::ServiceExt;

    struct MockQueryActor;

    impl Actor for MockQueryActor {
        type Msg = QueryMessage;
        type State = ();
        type Arguments = ();

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            _args: (),
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(())
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            message: Self::Msg,
            _state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            match message {
                QueryMessage::Query {
                    parsed_query,
                    limit: _,
                    reply,
                } => {
                    let _ = reply.send(vec![SearchResult {
                        url: "https://test.com".to_string(),
                        text: format!("Found: {}", parsed_query.semantic_text),
                        distance: 0.99,
                    }]);
                }
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_search_endpoint() {
        let (mock_ref, _) = Actor::spawn(None, MockQueryActor, ()).await.unwrap();

        let app = build_router(vec![mock_ref]);

        let request = Request::builder()
            .uri("/search?q=Rust&limit=5")
            .method("GET")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_json: Value = serde_json::from_slice(&body).unwrap();

        let results = body_json.as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["url"], "https://test.com");
        assert_eq!(results[0]["text"], "Found: Rust");
    }
}
