use ractor::BytesConvertable;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ManagerMessage {
    AddUrls(Vec<String>),
    RequestWork(String),
    UpdateDomainRules(String, Option<String>),
    DomainRateLimited(String, String),
    CrawlSuccess(String, String),
}

impl BytesConvertable for ManagerMessage {
    fn into_bytes(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
    fn from_bytes(bytes: Vec<u8>) -> Self {
        serde_json::from_slice(&bytes).unwrap()
    }
}
