use rusqlite::{Connection, params};
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

pub trait BlobStorageReader: Send + Sync {
    fn read_blob(
        &self,
        blob_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + '_>>;
}

pub struct DbBlobStorageReader {
    conn: Mutex<Connection>,
}

impl DbBlobStorageReader {
    pub fn new(db_path: &str) -> Result<Self, String> {
        if let Some(parent) = std::path::Path::new(db_path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        let conn = Connection::open(db_path).map_err(|e| format!("Failed to open DB: {}", e))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS blobs (
                id TEXT PRIMARY KEY,
                content BLOB
            )",
            [],
        )
        .map_err(|e| format!("Failed to create table: {}", e))?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl BlobStorageReader for DbBlobStorageReader {
    fn read_blob(
        &self,
        blob_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + '_>> {
        let id = blob_id.to_string();

        Box::pin(async move {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT content FROM blobs WHERE id = ?1")
                .map_err(|e| format!("Prepare Error: {}", e))?;

            let content: Vec<u8> = stmt
                .query_row(params![id], |row| row.get(0))
                .map_err(|e| format!("Query Error for {}: {}", id, e))?;

            Ok(content)
        })
    }
}
