use crate::actors::crawler::manager::messages::ManagerMessage;
use crate::actors::processor::messages::ProcessorMessage;
use ractor::ActorRef;
use reqwest::Client;

pub struct WorkerState {
    pub http_client: Client,
    pub manager_cluster: Vec<ActorRef<ManagerMessage>>,
    pub primary_manager: ActorRef<ManagerMessage>,
    pub processor_pool: Vec<ActorRef<ProcessorMessage>>,
}
