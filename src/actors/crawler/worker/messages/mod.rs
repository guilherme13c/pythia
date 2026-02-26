pub enum WorkerMessage {
    Fetch(String),
    FetchRobotsTxt { domain: String, url: String },
    NoWorkAvailable,
}
