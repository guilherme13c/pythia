use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub const CHUNK_MAX_WORDS: usize = 200;
pub const CHUNK_OVERLAP_SENTENCES: usize = 2;

pub struct TextChunker;

impl TextChunker {
    pub fn clean_text(raw_text: &str) -> String {
        raw_text.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    pub fn separate_sentences(clean_text: &str) -> Vec<String> {
        let mut sentences = Vec::new();
        let mut current_sentence = String::new();

        for c in clean_text.chars() {
            current_sentence.push(c);
            if c == '.' || c == '!' || c == '?' {
                sentences.push(current_sentence.trim().to_string());
                current_sentence.clear();
            }
        }
        if !current_sentence.trim().is_empty() {
            sentences.push(current_sentence.trim().to_string());
        }

        sentences
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
    model: Arc<Mutex<TextEmbedding>>,
}

impl Embedder {
    pub fn new(cache_dir: String) -> Result<Self, String> {
        let model = TextEmbedding::try_new(
            InitOptions::new(EmbeddingModel::AllMiniLML6V2)
                .with_cache_dir(PathBuf::from(cache_dir)),
        )
        .map_err(|e| format!("Failed to load fastembed model: {:?}", e))?;

        Ok(Self {
            model: Arc::new(Mutex::new(model)),
        })
    }

    pub fn embed_chunks(&self, chunks: Vec<String>) -> Result<Vec<Vec<f32>>, String> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        let mut model_guard = self.model.lock().unwrap();
        model_guard
            .embed(chunks, None)
            .map_err(|e| format!("Embedding failed: {:?}", e))
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
}
