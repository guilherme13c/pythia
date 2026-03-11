use fastembed::{EmbeddingModel, InitOptions, RerankInitOptions, TextEmbedding, TextRerank};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use unicode_segmentation::UnicodeSegmentation;

pub const CHUNK_MAX_WORDS: usize = 200;
pub const CHUNK_OVERLAP_SENTENCES: usize = 2;

pub struct TextChunker;

impl TextChunker {
    pub fn clean_text(raw_text: &str) -> String {
        raw_text.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    pub fn separate_sentences(clean_text: &str) -> Vec<String> {
        clean_text
            .unicode_sentences()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    pub fn chunk_text(clean_text: &str, max_words: usize, overlap_sentences: usize) -> Vec<String> {
        let sentences = Self::separate_sentences(clean_text);
        if sentences.is_empty() {
            return Vec::new();
        }

        let mut chunks = Vec::new();
        let mut i = 0;

        while i < sentences.len() {
            let (chunk_text, next_i) =
                Self::build_chunk(&sentences, i, max_words, overlap_sentences);
            chunks.push(chunk_text);

            if next_i >= sentences.len() || next_i <= i {
                break;
            }
            i = next_i;
        }

        chunks
    }

    fn build_chunk(
        sentences: &[String],
        start_idx: usize,
        max_words: usize,
        overlap_sentences: usize,
    ) -> (String, usize) {
        let mut chunk_words = 0;
        let mut chunk_sentences = Vec::new();
        let mut j = start_idx;

        while j < sentences.len() {
            let sentence = &sentences[j];
            let sentence_words = sentence.split_whitespace().count();

            if chunk_words + sentence_words > max_words && !chunk_sentences.is_empty() {
                break;
            }

            chunk_sentences.push(sentence.as_str());
            chunk_words += sentence_words;
            j += 1;
        }

        let next_i = if j == sentences.len() {
            j
        } else {
            std::cmp::max(start_idx + 1, j.saturating_sub(overlap_sentences))
        };

        (chunk_sentences.join(" "), next_i)
    }
}

#[derive(Clone)]
pub struct Embedder {
    embedding_model: Arc<Mutex<TextEmbedding>>,
    rerank_model: Arc<Mutex<TextRerank>>,
}

impl Embedder {
    pub fn new(cache_dir: String) -> Result<Self, String> {
        let cache_path = PathBuf::from(cache_dir);

        let embedding = TextEmbedding::try_new(
            InitOptions::new(EmbeddingModel::AllMiniLML6V2).with_cache_dir(cache_path.clone()),
        )
        .map_err(|e| format!("Failed to load fastembed model: {:?}", e))?;

        let reranker = TextRerank::try_new(RerankInitOptions::default().with_cache_dir(cache_path))
            .map_err(|e| format!("Failed to load rerank model: {:?}", e))?;

        Ok(Self {
            embedding_model: Arc::new(Mutex::new(embedding)),
            rerank_model: Arc::new(Mutex::new(reranker)),
        })
    }

    pub fn embed_chunks(&self, chunks: Vec<String>) -> Result<Vec<Vec<f32>>, String> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        let mut model_guard = self.embedding_model.lock().unwrap();

        model_guard
            .embed(chunks, None)
            .map_err(|e| format!("Embedding failed: {:?}", e))
    }

    pub fn rerank_documents(
        &self,
        query: &str,
        documents: Vec<String>,
    ) -> Result<Vec<f32>, String> {
        if documents.is_empty() {
            return Ok(Vec::new());
        }

        let doc_refs: Vec<&str> = documents.iter().map(|s| s.as_str()).collect();

        let mut model_guard = self.rerank_model.lock().unwrap();
        let results = model_guard
            .rerank(query, doc_refs, true, None)
            .map_err(|e| format!("Reranking failed: {:?}", e))?;

        let mut scores = vec![0.0; documents.len()];
        for res in results {
            scores[res.index] = res.score;
        }

        Ok(scores)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_text_removes_messy_whitespace() {
        let raw_html_text = "  This \n\n is   some \t\t very \n messy text.   ";
        let cleaned = TextChunker::clean_text(raw_html_text);
        assert_eq!(cleaned, "This is some very messy text.");
    }

    #[test]
    fn test_separate_sentences() {
        let text = "Hello world! How are you? I am fine. This has no punctuation";
        let sentences = TextChunker::separate_sentences(text);

        assert_eq!(sentences.len(), 4);
        assert_eq!(sentences[0], "Hello world!");
        assert_eq!(sentences[1], "How are you?");
        assert_eq!(sentences[2], "I am fine.");
        assert_eq!(sentences[3], "This has no punctuation");
    }

    #[test]
    fn test_chunk_text_semantic_boundaries() {
        let text = "One. Two. Three. Four. Five.";
        let chunks = TextChunker::chunk_text(text, 5, 1);

        assert_eq!(chunks.len(), 1, "Should group short sentences together");
        assert_eq!(chunks[0], "One. Two. Three. Four. Five.");
    }

    #[test]
    fn test_chunk_text_sliding_window_sentence_overlap() {
        let text = "Sentence one is here. Sentence two is short. Sentence three. Sentence four is longer. Sentence five.";
        let chunks = TextChunker::chunk_text(text, 8, 1);

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], "Sentence one is here. Sentence two is short.");
        assert_eq!(chunks[1], "Sentence two is short. Sentence three.");
        assert_eq!(
            chunks[2],
            "Sentence three. Sentence four is longer. Sentence five."
        );
    }

    #[test]
    fn test_separate_sentences_with_abbreviations() {
        let text = "Dr. Smith works at OpenAI in the U.S.A. He earns $1.5 million per year! Is that true? Yes, e.g. his tax returns show it.";
        let sentences = TextChunker::separate_sentences(text);

        assert_eq!(
            sentences.len(),
            5,
            "Should correctly handle decimals and lowercase abbreviations"
        );
        assert_eq!(sentences[0], "Dr.");
        assert_eq!(sentences[1], "Smith works at OpenAI in the U.S.A.");
        assert_eq!(sentences[2], "He earns $1.5 million per year!");
        assert_eq!(sentences[3], "Is that true?");
        assert_eq!(sentences[4], "Yes, e.g. his tax returns show it.");
    }
}
