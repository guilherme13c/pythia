use ractor::BytesConvertable;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum WorkerMessage {
    Fetch(String),
    FetchRobotsTxt(String, String),
    NoWorkAvailable,
}

impl BytesConvertable for WorkerMessage {
    fn into_bytes(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
    fn from_bytes(bytes: Vec<u8>) -> Self {
        serde_json::from_slice(&bytes).unwrap()
    }
}
