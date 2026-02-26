use ractor::{Actor, ActorProcessingErr, ActorRef};
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tracing::{debug, info, warn};
use url::Url;

use super::messages::WorkerMessage;
use super::state::WorkerState;
use crate::actors::crawler::manager::messages::ManagerMessage;
use crate::actors::crawler::manager::state::DomainMetadata;

fn get_shard_index(domain: &str, num_shards: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    domain.hash(&mut hasher);
    (hasher.finish() as usize) % num_shards
}

pub struct WorkerActor;

impl Actor for WorkerActor {
    type Msg = WorkerMessage;
    type State = WorkerState;
    type Arguments = (
        Vec<ActorRef<ManagerMessage>>,
        ActorRef<ManagerMessage>,
        // ActorRef<ProcessorMessage>,
    );

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        (manager_cluster, primary_manager /* processor_ref */): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Worker Actor starting up...");

        let client = Client::builder()
            .user_agent("PythiaSearchBot/1.0 (guilhermesccaporali@protonmail.com)")
            .timeout(Duration::from_secs(3))
            .build()
            .expect("Failed to build HTTP client");

        let _ = primary_manager.cast(ManagerMessage::RequestWork(myself.clone()));

        Ok(WorkerState {
            http_client: client,
            manager_cluster: manager_cluster,
            primary_manager: primary_manager,
            // processor: processor_ref,
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
                debug!("Worker fetching HTML: {}", url_str);
                let domain = match Url::parse(&url_str) {
                    Ok(u) => u.host_str().unwrap_or("unknown").to_string(),
                    Err(_) => "unknown".to_string(),
                };
                match state.http_client.get(&url_str).send().await {
                    Ok(response) if response.status().is_success() => {
                        let _ = state.primary_manager.cast(ManagerMessage::CrawlSuccess {
                            domain: domain.clone(),
                        });
                        if let Ok(html) = response.text().await {
                            let document = Html::parse_document(&html);

                            let mut routed_links: HashMap<usize, Vec<String>> = HashMap::new();
                            let num_shards = state.manager_cluster.len();

                            let link_selector = Selector::parse("a[href]")
                                .expect("Failed to parse hardcoded CSS selector");

                            if let Ok(base_url) = Url::parse(&url_str) {
                                for element in document.select(&link_selector) {
                                    if let Some(href) = element.value().attr("href") {
                                        if let Ok(mut absolute_url) = base_url.join(href) {
                                            absolute_url.set_fragment(None);

                                            if absolute_url.scheme() == "http"
                                                || absolute_url.scheme() == "https"
                                            {
                                                let target_domain =
                                                    absolute_url.host_str().unwrap_or("unknown");
                                                let target_shard =
                                                    get_shard_index(target_domain, num_shards);

                                                routed_links
                                                    .entry(target_shard)
                                                    .or_default()
                                                    .push(absolute_url.to_string());
                                            }
                                        }
                                    }
                                }
                            }

                            let _raw_text =
                                document.root_element().text().collect::<Vec<_>>().join(" ");

                            debug!("Extracted links and text from {}", url_str);

                            for (shard_idx, urls) in routed_links {
                                let _ = state.manager_cluster[shard_idx]
                                    .cast(ManagerMessage::AddUrls(urls));
                            }
                            // let _ = state.processor.cast(ProcessorMessage::ProcessRawDocument {
                            //     url: url_str.clone(),
                            //     raw_text
                            // });
                        }
                    }
                    Ok(response) if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                        let _ = state
                            .primary_manager
                            .cast(ManagerMessage::DomainRateLimited {
                                domain,
                                url: url_str.clone(),
                            });
                    }
                    Ok(response) => warn!(
                        "Failed to fetch {} - Status: {}",
                        url_str,
                        response.status()
                    ),
                    Err(e) => warn!("Network error fetching {}: {}", url_str, e),
                }

                let _ = state
                    .primary_manager
                    .cast(ManagerMessage::RequestWork(myself));
            }

            WorkerMessage::FetchRobotsTxt { domain, url } => {
                debug!("Worker fetching robots.txt for: {}", domain);

                let mut metadata = DomainMetadata::default_unfetched();
                metadata.rules_fetched = true;

                match state.http_client.get(&url).send().await {
                    Ok(response) if response.status().is_success() => {
                        if let Ok(text) = response.text().await {
                            let mut in_target_user_agent = false;

                            for line in text.lines() {
                                let clean_line = line.split('#').next().unwrap_or("").trim();
                                if clean_line.is_empty() {
                                    continue;
                                }

                                let lower_line = clean_line.to_lowercase();

                                if lower_line.starts_with("user-agent:") {
                                    let agent = lower_line.trim_start_matches("user-agent:").trim();
                                    in_target_user_agent =
                                        agent == "*" || agent.contains("pythiasearchbot");
                                    continue;
                                }

                                if in_target_user_agent {
                                    if lower_line.starts_with("disallow:") {
                                        let path = clean_line[9..].trim().to_string();
                                        if !path.is_empty() {
                                            metadata.disallowed_paths.push(path);
                                        }
                                    } else if lower_line.starts_with("allow:") {
                                        let path = clean_line[6..].trim().to_string();
                                        if !path.is_empty() {
                                            metadata.allowed_paths.push(path);
                                        }
                                    } else if lower_line.starts_with("crawl-delay:") {
                                        if let Ok(delay_secs) =
                                            clean_line[12..].trim().parse::<u64>()
                                        {
                                            metadata.crawl_delay = Duration::from_secs(delay_secs);
                                            debug!(
                                                "Found custom crawl delay for {}: {}s",
                                                domain, delay_secs
                                            );
                                        }
                                    }
                                }
                            }
                            info!("Successfully parsed robots.txt for {}", domain);
                        }
                    }
                    _ => {
                        debug!(
                            "No robots.txt found for {}, assuming default permissive rules.",
                            domain
                        );
                    }
                }

                let _ = state
                    .primary_manager
                    .cast(ManagerMessage::UpdateDomainRules { domain, metadata });
                let _ = state
                    .primary_manager
                    .cast(ManagerMessage::RequestWork(myself));
            }

            WorkerMessage::NoWorkAvailable => {
                tokio::time::sleep(Duration::from_millis(500)).await;

                let _ = state
                    .primary_manager
                    .cast(ManagerMessage::RequestWork(myself));
            }
        }
        Ok(())
    }
}
