use rusqlite::{Connection, params};
use std::pin::Pin;
use std::sync::Mutex;
use uuid::Uuid;

pub trait BlobStorage: Send + Sync {
    fn save_blob(
        &self,
        content: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + '_>>;
}

pub struct DbBlobStorage {
    conn: Mutex<Connection>,
}

impl DbBlobStorage {
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

    pub async fn save_blob(&self, content: Vec<u8>) -> Result<String, String> {
        let blob_id = Uuid::new_v4().to_string();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO blobs (id, content) VALUES (?1, ?2)",
            params![blob_id, content],
        )
        .map_err(|e| format!("DB Insert Error: {}", e))?;

        Ok(blob_id)
    }
}

impl BlobStorage for DbBlobStorage {
    fn save_blob(
        &self,
        content: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + '_>> {
        Box::pin(async move {
            let blob_id = Uuid::new_v4().to_string();
            let conn = self.conn.lock().unwrap();

            conn.execute(
                "INSERT INTO blobs (id, content) VALUES (?1, ?2)",
                params![blob_id, content],
            )
            .map_err(|e| format!("DB Insert Error: {}", e))?;

            Ok(blob_id)
        })
    }
}
