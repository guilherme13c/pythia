use fastembed::{EmbeddingModel, InitOptions, RerankInitOptions, TextEmbedding, TextRerank};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{error, info};

use crate::actors::indexer::messages::{IndexerMessage, SearchRequestPayload};
use crate::actors::query::messages::QueryNetworkMessage;
use crate::actors::query::state::PendingRequest;

use super::messages::{QueryMessage, SearchResult};
use super::state::QueryState;

pub struct QueryActor;

const MAX_CANDIDATE_LIMIT: usize = 100;

impl QueryActor {
    fn get_cache_dir() -> PathBuf {
        let path = env::var("FASTEMBED_CACHE_PATH")
            .unwrap_or_else(|_| "/app/models/fastembed".to_string());
        PathBuf::from(path)
    }

    fn initialize_model() -> TextEmbedding {
        TextEmbedding::try_new(
            InitOptions::new(EmbeddingModel::AllMiniLML6V2).with_cache_dir(Self::get_cache_dir()),
        )
        .unwrap()
    }

    fn initialize_reranker() -> TextRerank {
        TextRerank::try_new(RerankInitOptions::default().with_cache_dir(Self::get_cache_dir()))
            .unwrap()
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
        let mut rrf_scores: HashMap<(String, String), (f32, SearchResult)> = HashMap::new();

        for (rank, res) in vec_results.into_iter().enumerate() {
            let key = (res.url.clone(), res.text.clone());
            let score = 1.0 / (k + (rank as f32) + 1.0);
            let entry = rrf_scores.entry(key).or_insert((0.0, res));
            entry.0 += score;
        }

        for (rank, res) in fts_results.into_iter().enumerate() {
            let key = (res.url.clone(), res.text.clone());
            let score = 1.0 / (k + (rank as f32) + 1.0);
            let entry = rrf_scores.entry(key).or_insert((0.0, res));
            entry.0 += score;
        }

        let mut final_results: Vec<SearchResult> = rrf_scores
            .into_iter()
            .map(|(_, (combined_score, mut res))| {
                res.distance = combined_score;
                res
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

    fn rerank_candidates(
        reranker: &mut TextRerank,
        query_text: &str,
        mut candidates: Vec<SearchResult>,
        limit: usize,
    ) -> Vec<SearchResult> {
        if candidates.is_empty() {
            return candidates;
        }

        let document_texts: Vec<&str> = candidates.iter().map(|c| c.text.as_str()).collect();

        match reranker.rerank(query_text, document_texts, false, None) {
            Ok(results) => {
                for result in results {
                    candidates[result.index].distance = result.score;
                }

                candidates.sort_by(|a, b| {
                    b.distance
                        .partial_cmp(&a.distance)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
            Err(e) => {
                error!(
                    "Cross-encoder reranking failed: {}. Falling back to RRF scores.",
                    e
                );
            }
        }

        candidates.truncate(limit);
        candidates
    }

    fn generate_snippet(text: &str, query: &str) -> String {
        let terms: Vec<String> = query
            .to_lowercase()
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();

        if terms.is_empty() {
            let excerpt: String = text.chars().take(200).collect();
            return format!("{}...", excerpt);
        }

        let words: Vec<&str> = text.split_whitespace().collect();
        let window_size = 40;

        let mut best_score = 0;
        let mut best_start = 0;

        let limit = words.len().saturating_sub(window_size);
        for i in 0..=limit {
            let end = std::cmp::min(i + window_size, words.len());
            let window = &words[i..end];
            let mut score = 0;
            for word in window {
                let lower = word.to_lowercase();
                if terms.iter().any(|t| lower.contains(t)) {
                    score += 1;
                }
            }
            if score > best_score {
                best_score = score;
                best_start = i;
            }
        }

        let best_window = &words[best_start..std::cmp::min(best_start + window_size, words.len())];
        let mut highlighted = Vec::new();

        for w in best_window {
            let lower = w.to_lowercase();
            if terms.iter().any(|t| lower.contains(t)) {
                highlighted.push(format!("<b>{}</b>", w));
            } else {
                highlighted.push(w.to_string());
            }
        }

        let mut result = highlighted.join(" ");
        if best_start > 0 {
            result = format!("...{} ", result);
        }
        if best_start + window_size < words.len() {
            result = format!("{}...", result);
        }

        result
    }

    async fn finalize_query(
        reranker_arc: Arc<Mutex<TextRerank>>,
        req: PendingRequest,
        candidate_limit: usize,
    ) {
        let candidates =
            Self::compute_rrf(req.all_vec_results, req.all_fts_results, candidate_limit);
        let query_text = req.original_text.clone();
        let limit = req.limit;

        let final_results = tokio::task::spawn_blocking(move || {
            let mut reranker = reranker_arc.lock().unwrap();
            Self::rerank_candidates(&mut reranker, &query_text, candidates, limit)
        })
        .await
        .unwrap_or_else(|e| {
            error!("Reranking task panicked: {}", e);
            vec![]
        });

        let paged_results: Vec<SearchResult> = final_results
            .into_iter()
            .skip(req.offset)
            .take(req.limit)
            .map(|mut result| {
                result.snippet = Self::generate_snippet(&result.text, &req.original_text);
                result
            })
            .collect();

        let _ = req.reply_port.send(paged_results);
    }
}

impl Actor for QueryActor {
    type Msg = QueryMessage;
    type State = QueryState;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Starting Searcher Actor...");

        if let Some(name) = myself.get_name() {
            ractor::pg::join(name, vec![myself.clone().into()]);
        }

        Ok(QueryState {
            embedding_model: Arc::new(Mutex::new(Self::initialize_model())),
            reranker_model: Arc::new(Mutex::new(Self::initialize_reranker())),
            pending_requests: HashMap::new(),
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            QueryMessage::Query(parsed_query, limit, offset, reply) => {
                let request_id = uuid::Uuid::new_v4().to_string();

                let original_text = parsed_query.original_text.clone();
                let model_arc = state.embedding_model.clone();

                let embeddings = tokio::task::spawn_blocking(move || {
                    let mut model = model_arc.lock().unwrap();
                    model.embed(vec![original_text], None).unwrap()
                })
                .await
                .unwrap();

                let query_vector = embeddings[0].clone();

                let candidate_limit = std::cmp::max((limit + offset) * 5, MAX_CANDIDATE_LIMIT);

                let indexers = ractor::pg::get_members(&"indexers".to_string());
                let expected_replies = indexers.len();

                if expected_replies == 0 {
                    let _ = reply.send(vec![]);
                    return Ok(());
                }

                state.pending_requests.insert(
                    request_id.clone(),
                    PendingRequest {
                        reply_port: reply,
                        original_text: parsed_query.original_text.clone(),
                        limit,
                        offset,
                        replies_received: 0,
                        expected_replies,
                        all_vec_results: Vec::new(),
                        all_fts_results: Vec::new(),
                    },
                );

                for cell in indexers {
                    let indexer_ref: ActorRef<IndexerMessage> = cell.into();
                    let _ = indexer_ref.cast(IndexerMessage::SearchRequest(SearchRequestPayload {
                        request_id: request_id.clone(),
                        reply_to: myself.get_name().unwrap(),
                        query_vector: query_vector.clone(),
                        fts_query: parsed_query.processed_text.clone(),
                        site_filter: parsed_query.site_filter.clone(),
                        limit: candidate_limit,
                    }));
                }
            }
            QueryMessage::Network(QueryNetworkMessage::IndexerReply {
                request_id,
                shard_vec_results,
                shard_fts_results,
            }) => {
                let mut complete = false;
                if let Some(req) = state.pending_requests.get_mut(&request_id) {
                    req.all_vec_results.extend(shard_vec_results);
                    req.all_fts_results.extend(shard_fts_results);
                    req.replies_received += 1;
                    if req.replies_received >= req.expected_replies {
                        complete = true;
                    }
                }

                if complete {
                    if let Some(req) = state.pending_requests.remove(&request_id) {
                        let candidate_limit =
                            std::cmp::max((req.limit + req.offset) * 5, MAX_CANDIDATE_LIMIT);
                        let reranker_arc = state.reranker_model.clone();

                        Self::finalize_query(reranker_arc, req, candidate_limit).await;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastembed::RerankInitOptions;

    #[test]
    fn test_compute_rrf_boosts_common_results() {
        let vec_results = vec![
            SearchResult {
                url: "https://doc-a.com".to_string(),
                text: "Text A".to_string(),
                title: None,
                description: None,
                distance: 0.1,
                snippet: String::new(),
            },
            SearchResult {
                url: "https://doc-b.com".to_string(),
                text: "Text B".to_string(),
                title: None,
                description: None,
                distance: 0.3,
                snippet: String::new(),
            },
        ];

        let fts_results = vec![
            SearchResult {
                url: "https://doc-b.com".to_string(),
                text: "Text B".to_string(),
                title: None,
                description: None,
                distance: 15.0,
                snippet: String::new(),
            },
            SearchResult {
                url: "https://doc-c.com".to_string(),
                text: "Text C".to_string(),
                title: None,
                description: None,
                distance: 5.0,
                snippet: String::new(),
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

    #[test]
    fn test_rerank_candidates_reorders_by_semantic_relevance() {
        let mut reranker = fastembed::TextRerank::try_new(RerankInitOptions::default())
            .expect("Failed to init test reranker");

        let query = "what is the rust programming language?";

        let doc_a = SearchResult {
            url: "https://auto-repair.com".to_string(),
            text: "I have so much rust on my old car. The rust is eating through the metal."
                .to_string(),
            title: None,
            description: None,
            distance: 10.0,
            snippet: String::new(),
        };

        let doc_b = SearchResult {
            url: "https://rust-lang.org".to_string(),
            text: "Rust is a blazingly fast and memory-safe systems programming language."
                .to_string(),
            title: None,
            description: None,
            distance: 5.0,
            snippet: String::new(),
        };

        let candidates = vec![doc_a, doc_b];

        let reranked = QueryActor::rerank_candidates(&mut reranker, query, candidates, 2);

        assert_eq!(reranked.len(), 2);

        assert_eq!(reranked[0].url, "https://rust-lang.org");
        assert_eq!(reranked[1].url, "https://auto-repair.com");

        assert!(reranked[0].distance > reranked[1].distance);
    }

    #[test]
    fn test_generate_snippet() {
        let text = "This is a very long document. It has many words. We are adding extra filler text here just to make sure that the document exceeds the forty word limit imposed by our sliding window algorithm. Otherwise the whole thing gets captured. We want to find the specific part about rust programming. Rust is fast and safe. The rest of the document is boring and shouldn't be in the snippet.";
        let query = "rust programming";

        let snippet = QueryActor::generate_snippet(text, query);

        assert!(snippet.contains("<b>rust</b>"));
        assert!(snippet.contains("<b>Rust</b>"));
        assert!(snippet.contains("<b>programming.</b>"));

        assert!(!snippet.contains("very long document"));
        assert!(snippet.starts_with("..."));
        assert!(snippet.contains("We want to find"));
    }
}
