use std::future::Future;
use std::pin::Pin;

pub trait BlobStorageReader: Send + Sync {
    fn read_blob(
        &self,
        blob_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + '_>>;
}

pub struct MockBlobStorageReader;

impl BlobStorageReader for MockBlobStorageReader {
    fn read_blob(
        &self,
        blob_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + '_>> {
        let id = blob_id.to_string();
        Box::pin(async move {
            println!("💾 [Data Layer] Read mock blob from storage. ID: {}", id);
            Ok(b"<html><title>Mock Page</title><body>This is mock text.</body></html>".to_vec())
        })
    }
}
