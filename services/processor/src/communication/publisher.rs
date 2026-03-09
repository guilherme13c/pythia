use serde::Serialize;
use std::future::Future;
use std::pin::Pin;

#[derive(Serialize, Clone, Debug)]
pub struct VectorMessage {
    pub url: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub chunks: Vec<String>,
    pub embeddings: Vec<Vec<f32>>,
}

pub trait VectorPublisher: Send + Sync {
    fn publish(
        &self,
        message: VectorMessage,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;
}

pub struct MockVectorPublisher;

impl VectorPublisher for MockVectorPublisher {
    fn publish(
        &self,
        message: VectorMessage,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move {
            println!(
                "📈 [Comm Layer] Published {} vectors to queue for URL: {}",
                message.embeddings.len(),
                message.url
            );
            Ok(())
        })
    }
}
