use ractor::{Actor, ActorProcessingErr, ActorRef};
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;
use std::time::Duration;
use tracing::{debug, info, warn};
use url::Url;

use super::messages::WorkerMessage;
use super::state::WorkerState;
use crate::actors::crawler::manager::messages::ManagerMessage;
use crate::actors::crawler::manager::state::DomainMetadata;
use crate::actors::processor::messages::ProcessorMessage;

pub struct WorkerActor;

impl WorkerActor {
    async fn handle_fetch(
        &self,
        state: &mut WorkerState,
        myself: ActorRef<WorkerMessage>,
        url_str: String,
    ) {
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
                    let num_shards = state.manager_cluster.len();
                    let (routed_links, raw_text) =
                        extract_links_and_text(&html, &url_str, num_shards);

                    debug!("Extracted links and text from {}", url_str);

                    for (shard_idx, urls) in routed_links {
                        let _ =
                            state.manager_cluster[shard_idx].cast(ManagerMessage::AddUrls(urls));
                    }

                    let _ = state.processor.cast(ProcessorMessage::ProcessDocument {
                        url: url_str.clone(),
                        raw_text,
                    });
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

    async fn handle_fetch_robots_txt(
        &self,
        state: &mut WorkerState,
        myself: ActorRef<WorkerMessage>,
        domain: String,
        url: String,
    ) {
        debug!("Worker fetching robots.txt for: {}", domain);

        let metadata = match state.http_client.get(&url).send().await {
            Ok(response) if response.status().is_success() => {
                if let Ok(text) = response.text().await {
                    info!("Successfully parsed robots.txt for {}", domain);
                    parse_robots_txt(&text, &domain)
                } else {
                    let mut m = DomainMetadata::default_unfetched();
                    m.rules_fetched = true;
                    m
                }
            }
            _ => {
                debug!(
                    "No robots.txt found for {}, assuming default permissive rules.",
                    domain
                );
                let mut m = DomainMetadata::default_unfetched();
                m.rules_fetched = true;
                m
            }
        };

        let _ = state
            .primary_manager
            .cast(ManagerMessage::UpdateDomainRules { domain, metadata });
        let _ = state
            .primary_manager
            .cast(ManagerMessage::RequestWork(myself));
    }
}

impl Actor for WorkerActor {
    type Msg = WorkerMessage;
    type State = WorkerState;
    type Arguments = (
        Vec<ActorRef<ManagerMessage>>,
        ActorRef<ManagerMessage>,
        ActorRef<ProcessorMessage>,
    );

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        (manager_cluster, primary_manager, processor_ref): Self::Arguments,
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
            manager_cluster,
            primary_manager,
            processor: processor_ref,
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
            WorkerMessage::FetchRobotsTxt { domain, url } => {
                self.handle_fetch_robots_txt(state, myself, domain, url)
                    .await;
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

fn get_shard_index(domain: &str, num_shards: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    domain.hash(&mut hasher);
    (hasher.finish() as usize) % num_shards
}

fn get_link_selector() -> &'static Selector {
    static SELECTOR: OnceLock<Selector> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("a[href]").expect("Failed to parse CSS selector"))
}

fn extract_links_and_text(
    html: &str,
    base_url_str: &str,
    num_shards: usize,
) -> (HashMap<usize, Vec<String>>, String) {
    let document = Html::parse_document(html);
    let mut routed_links: HashMap<usize, Vec<String>> = HashMap::new();
    let link_selector = get_link_selector();

    if let Ok(base_url) = Url::parse(base_url_str) {
        for element in document.select(link_selector) {
            if let Some(href) = element.value().attr("href")
                && let Ok(mut absolute_url) = base_url.join(href)
            {
                absolute_url.set_fragment(None);

                if absolute_url.scheme() == "http" || absolute_url.scheme() == "https" {
                    let target_domain = absolute_url.host_str().unwrap_or("unknown");
                    let target_shard = get_shard_index(target_domain, num_shards);

                    routed_links
                        .entry(target_shard)
                        .or_default()
                        .push(absolute_url.to_string());
                }
            }
        }
    }

    let raw_text = document.root_element().text().collect::<Vec<_>>().join(" ");
    (routed_links, raw_text)
}

fn parse_robots_txt(text: &str, domain: &str) -> DomainMetadata {
    let mut metadata = DomainMetadata::default_unfetched();
    metadata.rules_fetched = true;

    let mut in_target_user_agent = false;
    let mut found_specific_bot = false;
    let mut is_parsing_rules = false;

    for line in text.lines() {
        let clean_line = line.split('#').next().unwrap_or("").trim();
        if clean_line.is_empty() {
            continue;
        }

        let lower_line = clean_line.to_lowercase();

        if lower_line.starts_with("user-agent:") {
            if is_parsing_rules {
                in_target_user_agent = false;
                is_parsing_rules = false;
            }

            let agent = lower_line.trim_start_matches("user-agent:").trim();

            if agent.contains("pythiasearchbot") {
                in_target_user_agent = true;
                if !found_specific_bot {
                    metadata.disallowed_paths.clear();
                    metadata.allowed_paths.clear();
                    metadata.crawl_delay = Duration::from_secs(2);
                    found_specific_bot = true;
                }
            } else if agent == "*" {
                if !found_specific_bot {
                    in_target_user_agent = true;
                }
            }
            continue;
        }

        if lower_line.starts_with("disallow:")
            || lower_line.starts_with("allow:")
            || lower_line.starts_with("crawl-delay:")
        {
            is_parsing_rules = true;

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
                } else if lower_line.starts_with("crawl-delay:")
                    && let Ok(delay_secs) = clean_line[12..].trim().parse::<u64>()
                {
                    metadata.crawl_delay = Duration::from_secs(delay_secs);
                    debug!("Found custom crawl delay for {}: {}s", domain, delay_secs);
                }
            }
        }
    }

    metadata
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_parse_robots_txt_universal_rules() {
        let robots_content = "
            # This is a comment
            User-agent: *
            Disallow: /admin/ # inline comment
            Crawl-delay: 5
            
            User-agent: googlebot
            Disallow: /secret/
        ";

        let metadata = parse_robots_txt(robots_content, "example.com");

        assert!(metadata.rules_fetched);
        assert_eq!(metadata.crawl_delay, Duration::from_secs(5));
        assert_eq!(metadata.disallowed_paths, vec!["/admin/"]);
        assert!(!metadata.disallowed_paths.contains(&"/secret/".to_string()));
    }

    #[test]
    fn test_parse_robots_txt_specific_bot_rules() {
        let robots_content = "
            User-agent: *
            Disallow: /everything/
            
            User-agent: PythiaSearchBot
            Allow: /everything/
            Disallow: /just-one-thing/
        ";

        let metadata = parse_robots_txt(robots_content, "example.com");

        assert_eq!(metadata.allowed_paths, vec!["/everything/"]);
        assert_eq!(metadata.disallowed_paths, vec!["/just-one-thing/"]);
    }

    #[test]
    fn test_extract_links_and_text() {
        let html = r#"
            <html>
                <body>
                    <p>Hello world!</p>
                    <a href="/about">About Us</a>
                    <a href="https://other.com/page">External</a>
                    <a href="mailto:test@example.com">Email</a>
                    <a href="/faq#section1">FAQ</a>
                </body>
            </html>
        "#;

        let base_url = "https://example.com/home";
        let num_shards = 3;

        let (routed_links, raw_text) = extract_links_and_text(html, base_url, num_shards);

        assert!(raw_text.contains("Hello world!"));
        assert!(raw_text.contains("About Us"));

        let mut all_links = Vec::new();
        for urls in routed_links.values() {
            all_links.extend(urls.clone());
        }

        assert_eq!(all_links.len(), 3);
        assert!(all_links.contains(&"https://example.com/about".to_string()));
        assert!(all_links.contains(&"https://other.com/page".to_string()));
        assert!(all_links.contains(&"https://example.com/faq".to_string()));
    }

    #[test]
    fn test_get_shard_index_determinism() {
        let domain = "wikipedia.org";
        let shard1 = get_shard_index(domain, 5);
        let shard2 = get_shard_index(domain, 5);

        assert_eq!(shard1, shard2);
    }
}
