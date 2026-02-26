use super::state::DomainMetadata;
use crate::actors::crawler::worker::messages::WorkerMessage;
use ractor::ActorRef;

pub enum ManagerMessage {
    AddUrls(Vec<String>),
    RequestWork(ActorRef<WorkerMessage>),
    UpdateDomainRules {
        domain: String,
        metadata: DomainMetadata,
    },
    DomainRateLimited {
        domain: String,
        url: String,
    },
    CrawlSuccess {
        domain: String,
    },
}
