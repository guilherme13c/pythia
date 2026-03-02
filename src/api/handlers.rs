use axum::{
    Json,
    extract::{Query, State},
};
use ractor::ActorRef;
use rand::seq::IndexedRandom;

use super::models::SearchParams;
use crate::actors::query::messages::{ParsedQuery, QueryMessage, SearchResult};

pub async fn search_handler(
    State(query_pool): State<Vec<ActorRef<QueryMessage>>>,
    Query(params): Query<SearchParams>,
) -> Json<Vec<SearchResult>> {
    let limit = params.limit.unwrap_or(10);

    let lang = params.lang.unwrap_or_else(|| "en".to_string());

    let parsed_query = ParsedQuery::parse(&params.q, lang.as_str());

    let query_ref = {
        let mut rng = rand::rng();
        query_pool
            .choose(&mut rng)
            .expect("Query pool is empty!")
            .clone()
    };

    let results = ractor::call!(query_ref, |reply| QueryMessage::Query {
        parsed_query,
        limit,
        reply,
    })
    .unwrap_or_else(|_| vec![]);

    Json(results)
}
