use headless_chrome::{Browser, LaunchOptions};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use url::Url;

use super::state::DynamicWorkerState;
use crate::actors::crawler::manager::messages::ManagerMessage;
use crate::actors::crawler::worker::common;
use crate::actors::crawler::worker::messages::WorkerMessage;

pub struct DynamicWorkerActor;

impl DynamicWorkerActor {
    async fn handle_fetch(
        &self,
        state: &mut DynamicWorkerState,
        myself: ActorRef<WorkerMessage>,
        url: String,
    ) {
        debug!("Dynamic Worker fetching URL via headless browser: {}", url);
        let domain = Url::parse(&url)
            .map(|u| u.host_str().unwrap_or("unknown").to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        let browser = state.browser.clone();
        let url_clone = url.clone();

        let html_result = tokio::task::spawn_blocking(move || -> Result<String, String> {
            let tab = browser.new_tab().map_err(|e| e.to_string())?;
            tab.navigate_to(&url_clone).map_err(|e| e.to_string())?;
            tab.wait_until_navigated().map_err(|e| e.to_string())?;
            std::thread::sleep(Duration::from_secs(2));
            tab.get_content().map_err(|e| e.to_string())
        })
        .await;

        match html_result {
            Ok(Ok(html)) => {
                self.report_success(state, &domain, &url);
                let (links, text, title, description) = common::extract_content(&html, &url);
                common::send_to_processor(url.clone(), text, title, description);
                common::route_new_links(links);
            }
            Ok(Err(e)) => warn!("Headless browser error fetching {}: {}", url, e),
            Err(e) => warn!("Task join error for {}: {}", url, e),
        }
        self.request_more_work(state, myself);
    }

    async fn handle_fetch_robots_txt(
        &self,
        state: &mut DynamicWorkerState,
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

    fn report_success(&self, state: &DynamicWorkerState, domain: &str, url: &str) {
        let manager_group = format!("manager-shard-{}", state.shard_idx);
        if let Some(cell) = ractor::pg::get_members(&manager_group).first() {
            let manager: ActorRef<ManagerMessage> = cell.clone().into();
            let _ = manager.cast(ManagerMessage::CrawlSuccess(
                domain.to_string(),
                url.to_string(),
            ));
        }
    }

    fn request_more_work(&self, state: &DynamicWorkerState, myself: ActorRef<WorkerMessage>) {
        let manager_group = format!("manager-shard-{}", state.shard_idx);
        if let Some(cell) = ractor::pg::get_members(&manager_group).first() {
            let manager: ActorRef<ManagerMessage> = cell.clone().into();
            let _ = manager.cast(ManagerMessage::RequestWork(myself.get_name().unwrap()));
        }
    }
}

impl Actor for DynamicWorkerActor {
    type Msg = WorkerMessage;
    type State = DynamicWorkerState;
    type Arguments = usize;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        shard_idx: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Dynamic Worker Actor starting up...");

        let browser = Browser::new(
            LaunchOptions::default_builder()
                .headless(true)
                .build()
                .expect("Failed to build LaunchOptions"),
        )
        .expect("Failed to launch headless chrome");

        let http_client = Client::builder()
            .user_agent("PythiaSearchBot/1.0 (Dynamic)")
            .timeout(Duration::from_secs(3))
            .build()
            .expect("Failed to build HTTP client");

        let manager_group = format!("manager-shard-{}", shard_idx);
        if let Some(cell) = ractor::pg::get_members(&manager_group).first() {
            let manager: ActorRef<ManagerMessage> = cell.clone().into();
            let _ = manager.cast(ManagerMessage::RequestWork(myself.get_name().unwrap()));
        }

        Ok(DynamicWorkerState {
            browser: Arc::new(browser),
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
        match message {
            WorkerMessage::Fetch(url_str) => {
                self.handle_fetch(state, myself, url_str).await;
            }
            WorkerMessage::FetchRobotsTxt(domain, url) => {
                self.handle_fetch_robots_txt(state, myself, domain, url)
                    .await;
            }
            WorkerMessage::NoWorkAvailable => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                self.request_more_work(state, myself);
            }
        }
        Ok(())
    }
}
