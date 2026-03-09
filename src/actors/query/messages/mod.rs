use ractor::{BytesConvertable, RpcReplyPort};
use rust_stemmers::{Algorithm, Stemmer};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use stop_words::{LANGUAGE, get};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchResult {
    pub url: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub text: String,
    pub distance: f32,
    pub snippet: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ParsedQuery {
    pub original_text: String,
    pub processed_text: String,
    pub site_filter: Option<String>,
    pub language: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum QueryNetworkMessage {
    IndexerReply {
        request_id: String,
        shard_vec_results: Vec<SearchResult>,
        shard_fts_results: Vec<SearchResult>,
    },
}

#[derive(Debug)]
pub enum QueryMessage {
    Query(ParsedQuery, usize, usize, RpcReplyPort<Vec<SearchResult>>),
    Network(QueryNetworkMessage),
}

impl BytesConvertable for QueryMessage {
    fn into_bytes(self) -> Vec<u8> {
        match self {
            QueryMessage::Query(..) => panic!("Local Query cannot cross network boundary"),
            QueryMessage::Network(n) => serde_json::to_vec(&n).unwrap(),
        }
    }
    fn from_bytes(bytes: Vec<u8>) -> Self {
        QueryMessage::Network(serde_json::from_slice(&bytes).unwrap())
    }
}

fn get_cached_stop_words(lang_code: &str) -> &'static HashSet<String> {
    static STOP_WORDS_CACHE: OnceLock<HashMap<String, HashSet<String>>> = OnceLock::new();

    let cache = STOP_WORDS_CACHE.get_or_init(|| {
        let mut map = HashMap::new();
        let supported_langs = vec![
            ("en", LANGUAGE::English),
            ("es", LANGUAGE::Spanish),
            ("fr", LANGUAGE::French),
            ("de", LANGUAGE::German),
            ("pt", LANGUAGE::Portuguese),
            ("it", LANGUAGE::Italian),
            ("nl", LANGUAGE::Dutch),
        ];

        for (code, lang_enum) in supported_langs {
            let word_set: HashSet<String> = get(lang_enum).iter().map(|s| s.to_string()).collect();
            map.insert(code.to_string(), word_set);
        }
        map
    });

    cache
        .get(lang_code)
        .unwrap_or_else(|| cache.get("en").unwrap())
}

impl ParsedQuery {
    pub fn parse(raw_query: &str, lang_code: &str) -> Self {
        let mut original_parts = Vec::new();
        let mut processed_parts = Vec::new();
        let mut site_filter = None;

        let stop_words = get_cached_stop_words(lang_code);

        let stemmer_algo = match lang_code {
            "es" => Algorithm::Spanish,
            "fr" => Algorithm::French,
            "de" => Algorithm::German,
            "pt" => Algorithm::Portuguese,
            "it" => Algorithm::Italian,
            "nl" => Algorithm::Dutch,
            _ => Algorithm::English,
        };
        let stemmer = Stemmer::create(stemmer_algo);

        for token in raw_query.split_whitespace() {
            if let Some(site) = token.strip_prefix("site:") {
                site_filter = Some(site.to_string());
            } else {
                original_parts.push(token);

                let lower_token = token.to_lowercase();

                if !stop_words.contains(lower_token.as_str()) {
                    let stemmed = stemmer.stem(&lower_token).to_string();
                    processed_parts.push(stemmed);
                }
            }
        }

        Self {
            original_text: original_parts.join(" "),
            processed_text: processed_parts.join(" "),
            site_filter,
            language: lang_code.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsed_query_extracts_site_filter() {
        let raw_query = "how to build a search engine site:rust-lang.org";
        let parsed = ParsedQuery::parse(raw_query, "en");

        assert_eq!(parsed.original_text, "how to build a search engine");
        assert_eq!(parsed.site_filter, Some("rust-lang.org".to_string()));
    }

    #[test]
    fn test_parsed_query_removes_english_stopwords() {
        let raw_query = "what is the quick brown fox";
        let parsed = ParsedQuery::parse(raw_query, "en");

        assert!(!parsed.processed_text.contains("what"));
        assert!(!parsed.processed_text.contains("is"));
        assert!(!parsed.processed_text.contains("the"));

        assert!(parsed.processed_text.contains("quick"));
        assert!(parsed.processed_text.contains("brown"));
        assert!(parsed.processed_text.contains("fox"));
    }

    #[test]
    fn test_parsed_query_multilingual_stemming() {
        let raw_query_fr = "les chats mangent";
        let parsed_fr = ParsedQuery::parse(raw_query_fr, "fr");

        assert!(!parsed_fr.processed_text.contains("les"));
        assert!(parsed_fr.processed_text.contains("chat"));
        assert!(parsed_fr.processed_text.contains("mang"));
    }
}
