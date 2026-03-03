use reqwest::Client;

pub struct WorkerState {
    pub http_client: Client,
    pub shard_idx: usize,
    pub num_manager_shards: usize,
}
