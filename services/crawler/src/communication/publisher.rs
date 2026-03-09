use serde::Serialize;
use std::future::Future;
use std::pin::Pin;

#[derive(Serialize)]
pub struct DocumentMessage {
    pub url: String,
    pub blob_id: String,
    pub mime_type: String,
}

pub trait DocumentPublisher: Send + Sync {
    fn publish(
        &self,
        message: DocumentMessage,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;
}

pub struct MockPublisher;

impl DocumentPublisher for MockPublisher {
    fn publish(
        &self,
        message: DocumentMessage,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move {
            println!(
                "📫 [Comm Layer] Published message to queue: URL: {} -> BLOB_ID: {}",
                message.url, message.blob_id
            );
            Ok(())
        })
    }
}
