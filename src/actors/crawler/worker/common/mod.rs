use ractor::ActorRef;
use rand::seq::IndexedRandom;
use scraper::Selector;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;
use std::time::Duration;
use tracing::{debug, error, info};
use url::Url;

use crate::actors::crawler::manager::messages::ManagerMessage;
use crate::actors::crawler::manager::state::DomainMetadata;
use crate::actors::processor::messages::ProcessorMessage;

pub fn route_new_links(links: Vec<String>) {
    let mut managers = ractor::pg::get_members(&"crawler_managers".to_string());
    if managers.is_empty() {
        return;
    }

    managers.sort_by_key(|m| {
        m.get_name()
            .unwrap_or_default()
            .split('-')
            .next_back()
            .unwrap_or("0")
            .parse::<usize>()
            .unwrap_or(0)
    });

    let mut routed_batches: HashMap<usize, Vec<String>> = HashMap::new();

    for link in links {
        if let Ok(parsed) = Url::parse(&link) {
            let domain = parsed.host_str().unwrap_or("unknown");
            let shard_idx = get_shard_index(domain, managers.len());
            routed_batches.entry(shard_idx).or_default().push(link);
        }
    }

    for (shard_idx, urls) in routed_batches {
        let manager_ref: ActorRef<ManagerMessage> = managers[shard_idx].clone().into();
        let _ = manager_ref.cast(ManagerMessage::AddUrls(urls));
    }
}

pub fn extract_content(
    html: &str,
    base_url_str: &str,
) -> (Vec<String>, String, Option<String>, Option<String>) {
    let document = scraper::Html::parse_document(html);
    let link_selector = get_link_selector();
    let mut links = Vec::new();

    if let Ok(base_url) = Url::parse(base_url_str) {
        for element in document.select(link_selector) {
            if let Some(href) = element.value().attr("href")
                && let Ok(mut absolute_url) = base_url.join(href)
            {
                absolute_url.set_fragment(None);
                if absolute_url.scheme() == "http" || absolute_url.scheme() == "https" {
                    links.push(absolute_url.to_string());
                }
            }
        }
    }

    let raw_text = document.root_element().text().collect::<Vec<_>>().join(" ");
    let title = document
        .select(&Selector::parse("title").unwrap())
        .next()
        .map(|t| t.inner_html().trim().to_string())
        .filter(|s| !s.is_empty());
    let description = document
        .select(&Selector::parse("meta[name=\"description\"]").unwrap())
        .next()
        .and_then(|m| m.value().attr("content"))
        .map(|c| c.trim().to_string())
        .filter(|s| !s.is_empty());

    (links, raw_text, title, description)
}

pub fn extract_pdf_metadata(bytes: &[u8], url_str: &str) -> (Option<String>, Option<String>) {
    let mut title = None;
    let mut description = None;

    if let Ok(doc) = lopdf::Document::load_mem(bytes)
        && let Ok(info_ref) = doc.trailer.get(b"Info")
        && let Ok(info_dict) = doc
            .get_object(info_ref.as_reference().unwrap_or((0, 0)))
            .and_then(|obj| obj.as_dict())
    {
        if let Ok(t_bytes) = info_dict.get(b"Title").and_then(|obj| obj.as_str()) {
            let trimmed = String::from_utf8_lossy(t_bytes).trim().to_string();
            if !trimmed.is_empty() {
                title = Some(trimmed);
            }
        }
        if let Ok(s_bytes) = info_dict.get(b"Subject").and_then(|obj| obj.as_str()) {
            let trimmed = String::from_utf8_lossy(s_bytes).trim().to_string();
            if !trimmed.is_empty() {
                description = Some(trimmed);
            }
        }
    }

    if title.is_none()
        && let Ok(parsed_url) = Url::parse(url_str)
        && let Some(segments) = parsed_url.path_segments()
        && let Some(last) = segments.last()
    {
        let fallback_title = last.replace(".pdf", "").replace(['-', '_'], " ");
        if !fallback_title.trim().is_empty() {
            title = Some(fallback_title);
        }
    }

    (title, description)
}

pub fn extract_xml(xml: &str) -> (String, Option<String>, Option<String>) {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut text_content = Vec::new();
    let mut title = None;
    let mut description = None;
    let mut current_tag = String::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                current_tag = String::from_utf8_lossy(e.name().as_ref()).to_lowercase()
            }
            Ok(Event::Text(e)) => {
                let text = String::from_utf8_lossy(e.as_ref());
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    text_content.push(trimmed.to_string());
                    if current_tag == "title" && title.is_none() {
                        title = Some(trimmed.to_string());
                    } else if (current_tag == "description" || current_tag == "summary")
                        && description.is_none()
                    {
                        description = Some(trimmed.to_string());
                    }
                }
            }
            Ok(Event::CData(e)) => {
                let text = String::from_utf8_lossy(e.as_ref());
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    text_content.push(trimmed.to_string());
                    if current_tag == "title" && title.is_none() {
                        title = Some(trimmed.to_string());
                    } else if (current_tag == "description" || current_tag == "summary")
                        && description.is_none()
                    {
                        description = Some(trimmed.to_string());
                    }
                }
            }
            Ok(Event::End(_)) => current_tag.clear(),
            Ok(Event::Eof) | Err(_) => break,
            _ => (),
        }
        buf.clear();
    }
    (text_content.join(" "), title, description)
}

pub fn parse_robots_txt(text: &str, domain: &str) -> DomainMetadata {
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
            } else if agent == "*" && !found_specific_bot {
                in_target_user_agent = true;
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

fn get_shard_index(domain: &str, num_shards: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    domain.hash(&mut hasher);
    (hasher.finish() as usize) % num_shards
}

fn get_link_selector() -> &'static Selector {
    static SELECTOR: OnceLock<Selector> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("a[href]").expect("Failed to parse CSS selector"))
}

pub fn send_to_processor(
    url: String,
    raw_text: String,
    title: Option<String>,
    description: Option<String>,
) {
    let processors = ractor::pg::get_members(&"processors".to_string());
    if let Some(cell) = processors.choose(&mut rand::rng()) {
        let processor_ref: ActorRef<ProcessorMessage> = cell.clone().into();
        match processor_ref.cast(ProcessorMessage::ProcessDocument(
            url.clone(),
            raw_text,
            title,
            description,
        )) {
            Ok(_) => info!("Successfully dispatched document to processor: {}", url),
            Err(e) => error!(
                "Failed to cast document to network processor {}: {:?}",
                url, e
            ),
        }
    } else {
        error!(
            "Dropped document {} because the 'processors' process group is empty!",
            url
        );
    }
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
    fn test_get_shard_index_determinism() {
        let domain = "wikipedia.org";
        let shard1 = get_shard_index(domain, 5);
        let shard2 = get_shard_index(domain, 5);

        assert_eq!(shard1, shard2);
    }

    #[test]
    fn test_extract_content() {
        let html = r#"
            <html>
                <head>
                    <title>My Awesome Page</title>
                    <meta name="description" content="This is a test description for the page.">
                </head>
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

        let (links, raw_text, title, description) = extract_content(html, base_url);

        assert_eq!(title, Some("My Awesome Page".to_string()));
        assert_eq!(
            description,
            Some("This is a test description for the page.".to_string())
        );

        assert!(raw_text.contains("Hello world!"));
        assert!(raw_text.contains("About Us"));

        assert_eq!(
            links.len(),
            3,
            "Should ignore mailto: links and normalize the rest"
        );
        assert!(links.contains(&"https://example.com/about".to_string()));
        assert!(links.contains(&"https://other.com/page".to_string()));
        assert!(
            links.contains(&"https://example.com/faq".to_string()),
            "Should strip URL fragments"
        );
    }

    #[test]
    fn test_extract_xml() {
        let xml = r#"
            <?xml version="1.0" encoding="UTF-8"?>
            <bookstore>
                <book category="cooking">
                    <title lang="en">Everyday Italian</title>
                    <author>Giada De Laurentiis</author>
                    <year>2005</year>
                    <price>30.00</price>
                    <summary><![CDATA[A great book about <b>Italian</b> cooking.]]></summary>
                </book>
            </bookstore>
        "#;

        let (extracted_text, title, description) = extract_xml(xml);

        assert_eq!(title, Some("Everyday Italian".to_string()));
        assert_eq!(
            description,
            Some("A great book about <b>Italian</b> cooking.".to_string())
        );

        assert!(extracted_text.contains("Everyday Italian"));
        assert!(extracted_text.contains("Giada De Laurentiis"));
        assert!(extracted_text.contains("2005"));
        assert!(extracted_text.contains("30.00"));
        assert!(extracted_text.contains("A great book about <b>Italian</b> cooking."));

        assert!(!extracted_text.contains("<book>"));
        assert!(!extracted_text.contains("</author>"));
    }
}
