use axum::{Router, routing::get};
use ractor::ActorRef;
use std::sync::Arc;
use tower_governor::{
    GovernorLayer, governor::GovernorConfigBuilder, key_extractor::SmartIpKeyExtractor,
};

use crate::actors::query::messages::QueryMessage;

pub mod handlers;
pub mod models;

pub fn build_router(query_pool: Vec<ActorRef<QueryMessage>>) -> Router {
    let governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .per_millisecond(200)
            .burst_size(5)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .unwrap(),
    );

    Router::new()
        .route("/health", get(handlers::health_handler))
        .route("/search", get(handlers::search_handler))
        .layer(GovernorLayer::new(governor_conf))
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
                QueryMessage::Query(parsed_query, _limit, _offset, reply) => {
                    let _ = reply.send(vec![SearchResult {
                        url: "https://test.com".to_string(),
                        text: format!("Found: {}", parsed_query.original_text),
                        distance: 0.99,
                        snippet: "<b>Rust</b> is awesome".to_string(),
                    }]);
                }
                QueryMessage::Network(_) => {}
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_search_endpoint() {
        let (mock_ref, _) = Actor::spawn(None, MockQueryActor, ()).await.unwrap();

        let app = build_router(vec![mock_ref]);

        let mut request = Request::builder()
            .uri("/search?q=Rust&limit=5")
            .method("GET")
            .header("X-Forwarded-For", "203.0.113.195")
            .body(Body::empty())
            .unwrap();

        request
            .extensions_mut()
            .insert(axum::extract::ConnectInfo(std::net::SocketAddr::from((
                [127, 0, 0, 1],
                8080,
            ))));

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
