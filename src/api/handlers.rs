use axum::{Json, extract::Query, extract::State};
use ractor::ActorRef;
use rand::seq::IndexedRandom;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::models::SearchParams;
use crate::actors::crawler::worker::common;
use crate::actors::query::messages::{ParsedQuery, QueryMessage, SearchResult};

#[derive(Deserialize, Serialize)]
pub struct CrawlParams {
    pub url: String,
}

pub async fn crawl_handler(Json(params): Json<CrawlParams>) -> Json<String> {
    info!("API received request to crawl: {}", params.url);

    common::route_new_links(vec![params.url.clone()]);

    Json(format!(
        "Successfully routed {} to the crawler network!",
        params.url
    ))
}

pub async fn search_handler(
    State(query_pool): State<Vec<ActorRef<QueryMessage>>>,
    Query(params): Query<SearchParams>,
) -> Json<Vec<SearchResult>> {
    let limit = params.limit.unwrap_or(10);
    let offset = params.offset.unwrap_or(0);

    let lang = params.lang.unwrap_or_else(|| "en".to_string());

    let parsed_query = ParsedQuery::parse(&params.q, lang.as_str());

    let query_ref = {
        let mut rng = rand::rng();
        query_pool
            .choose(&mut rng)
            .expect("Query pool is empty!")
            .clone()
    };

    let results = ractor::call!(query_ref, |reply| QueryMessage::Query(
        parsed_query,
        limit,
        offset,
        reply,
    ))
    .unwrap_or_else(|_| vec![]);

    Json(results)
}

pub async fn health_handler() -> Json<bool> {
    Json(true)
}
