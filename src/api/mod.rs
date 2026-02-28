use axum::{Router, routing::get};
use ractor::ActorRef;

use crate::actors::query::messages::QueryMessage;

pub mod handlers;
pub mod models;

pub fn build_router(query_ref: ActorRef<QueryMessage>) -> Router {
    Router::new()
        .route("/search", get(handlers::search_handler))
        .with_state(query_ref)
}
