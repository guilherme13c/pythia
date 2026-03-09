use ractor::{Actor, ActorProcessingErr, ActorRef};
use reqwest::Client;
use std::time::Duration;
use tracing::{debug, info, warn};
use url::Url;

use super::state::WorkerState;
use crate::actors::crawler::manager::messages::ManagerMessage;
use crate::actors::crawler::worker::common;
use crate::actors::crawler::worker::messages::WorkerMessage;

pub struct WorkerActor;

impl WorkerActor {
    async fn handle_fetch(
        &self,
        state: &mut WorkerState,
        myself: ActorRef<WorkerMessage>,
        url: String,
    ) {
        debug!("Worker fetching HTML: {}", url);
        let domain = Url::parse(&url)
            .map(|u| u.host_str().unwrap_or("unknown").to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        match state.http_client.get(&url).send().await {
            Ok(response) if response.status().is_success() => {
                self.report_success(state, &domain, &url);
                let content_type = response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|val| val.to_str().ok())
                    .unwrap_or("text/html")
                    .to_lowercase();

                if content_type.contains("application/pdf") {
                    if let Ok(bytes) = response.bytes().await
                        && let Ok(text) = pdf_extract::extract_text_from_mem(&bytes)
                    {
                        let (title, description) = common::extract_pdf_metadata(&bytes, &url);
                        common::send_to_processor(url.clone(), text, title, description);
                    }
                } else if content_type.contains("xml") {
                    if let Ok(xml_str) = response.text().await {
                        let (text, title, description) = common::extract_xml(&xml_str);
                        common::send_to_processor(url.clone(), text, title, description);
                    }
                } else if let Ok(html) = response.text().await {
                    let (links, text, title, description) = common::extract_content(&html, &url);
                    common::send_to_processor(url.clone(), text, title, description);
                    common::route_new_links(links);
                }
            }
            Ok(response) if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                self.report_rate_limit(state, &domain, &url);
            }
            Ok(response) => warn!("Failed to fetch {} - Status: {}", url, response.status()),
            Err(e) => warn!("Network error fetching {}: {:?}", url, e),
        }
        self.request_more_work(state, myself);
    }

    async fn handle_fetch_robots_txt(
        &self,
        state: &mut WorkerState,
        myself: ActorRef<WorkerMessage>,
        domain: String,
        url: String,
    ) {
        let robots_txt = match state.http_client.get(&url).send().await {
            Ok(response) if response.status().is_success() => response.text().await.ok(),
            _ => None,
        };
        let manager_group = format!("manager-shard-{}", state.shard_idx);
        if let Some(cell) = ractor::pg::get_members(&manager_group).first() {
            let manager: ActorRef<ManagerMessage> = cell.clone().into();
            let _ = manager.cast(ManagerMessage::UpdateDomainRules(domain, robots_txt));
            let _ = manager.cast(ManagerMessage::RequestWork(myself.get_name().unwrap()));
        }
    }

    fn report_success(&self, state: &WorkerState, domain: &str, url: &str) {
        let manager_group = format!("manager-shard-{}", state.shard_idx);
        if let Some(cell) = ractor::pg::get_members(&manager_group).first() {
            let manager: ActorRef<ManagerMessage> = cell.clone().into();
            let _ = manager.cast(ManagerMessage::CrawlSuccess(
                domain.to_string(),
                url.to_string(),
            ));
        }
    }

    fn report_rate_limit(&self, state: &WorkerState, domain: &str, url: &str) {
        let manager_group = format!("manager-shard-{}", state.shard_idx);
        if let Some(cell) = ractor::pg::get_members(&manager_group).first() {
            let manager: ActorRef<ManagerMessage> = cell.clone().into();
            let _ = manager.cast(ManagerMessage::DomainRateLimited(
                domain.to_string(),
                url.to_string(),
            ));
        }
    }

    fn request_more_work(&self, state: &mut WorkerState, myself: ActorRef<WorkerMessage>) {
        let manager_group = format!("manager-shard-{}", state.shard_idx);
        if let Some(cell) = ractor::pg::get_members(&manager_group).first() {
            let manager: ActorRef<ManagerMessage> = cell.clone().into();
            let _ = manager.cast(ManagerMessage::RequestWork(myself.get_name().unwrap()));
        }
    }
}

impl Actor for WorkerActor {
    type Msg = WorkerMessage;
    type State = WorkerState;
    type Arguments = usize;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        shard_idx: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Static Worker Actor starting up...");
        let http_client = Client::builder()
            .user_agent("PythiaSearchBot/1.0")
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        let manager_group = format!("manager-shard-{}", shard_idx);
        if let Some(cell) = ractor::pg::get_members(&manager_group).first() {
            let manager: ActorRef<ManagerMessage> = cell.clone().into();
            let _ = manager.cast(ManagerMessage::RequestWork(myself.get_name().unwrap()));
        }

        Ok(WorkerState {
            http_client,
            shard_idx,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!("received message: {:?}", message);
        match message {
            WorkerMessage::Fetch(url_str) => self.handle_fetch(state, myself, url_str).await,
            WorkerMessage::FetchRobotsTxt(domain, url) => {
                self.handle_fetch_robots_txt(state, myself, domain, url)
                    .await
            }
            WorkerMessage::NoWorkAvailable => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                self.request_more_work(state, myself);
            }
        }
        Ok(())
    }
}
