use std::future::Future;
use std::pin::Pin;

pub trait BlobStorage: Send + Sync {
    fn save_blob(
        &self,
        content: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + '_>>;
}

pub struct MockBlobStorage;

impl BlobStorage for MockBlobStorage {
    fn save_blob(
        &self,
        _content: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + '_>> {
        Box::pin(async move {
            let blob_id = uuid::Uuid::new_v4().to_string();
            println!(
                "💾 [Data Layer] Saved HTML payload to blob storage. ID: {}",
                blob_id
            );
            Ok(blob_id)
        })
    }
}
