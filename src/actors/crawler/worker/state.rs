use ractor::ActorRef;
use reqwest::Client;

use crate::actors::crawler::manager::messages::ManagerMessage;
use crate::actors::processor::messages::ProcessorMessage;

pub struct WorkerState {
    pub http_client: Client,
    pub manager_cluster: Vec<ActorRef<ManagerMessage>>,
    pub primary_manager: ActorRef<ManagerMessage>,
    pub processor: ActorRef<ProcessorMessage>,
}
