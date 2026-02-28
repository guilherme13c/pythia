use axum::{
    Json,
    extract::{Query, State},
};
use ractor::ActorRef;

use super::models::SearchParams;
use crate::actors::query::messages::{QueryMessage, SearchResult};

pub async fn search_handler(
    State(query_ref): State<ActorRef<QueryMessage>>,
    Query(params): Query<SearchParams>,
) -> Json<Vec<SearchResult>> {
    let limit = params.limit.unwrap_or(10);

    let results = ractor::call!(query_ref, |reply| QueryMessage::Query {
        text: params.q,
        limit,
        reply,
    })
    .unwrap_or_else(|_| vec![]);

    Json(results)
}
