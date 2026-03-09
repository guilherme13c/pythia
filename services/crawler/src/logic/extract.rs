use scraper::{Html, Selector};
use std::sync::OnceLock;
use std::time::Duration;
use url::Url;

fn get_link_selector() -> &'static Selector {
    static SELECTOR: OnceLock<Selector> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("a[href]").expect("Failed to parse CSS selector"))
}

pub fn extract_links(html: &str, base_url_str: &str) -> Vec<String> {
    let document = Html::parse_document(html);
    let link_selector = get_link_selector();
    let mut links = Vec::new();

    if let Ok(mut base_url) = Url::parse(base_url_str) {
        base_url.set_fragment(None);

        for element in document.select(link_selector) {
            if let Some(href) = element.value().attr("href") {
                if let Ok(mut absolute_url) = base_url.join(href) {
                    absolute_url.set_fragment(None);

                    if (absolute_url.scheme() == "http" || absolute_url.scheme() == "https")
                        && absolute_url != base_url
                    {
                        links.push(absolute_url.to_string());
                    }
                }
            }
        }
    }

    links.sort();
    links.dedup();

    links
}

pub fn parse_robots_txt(text: &str) -> (Vec<String>, Vec<String>, Option<Duration>) {
    let mut disallowed = Vec::new();
    let mut allowed = Vec::new();
    let mut delay = None;
    let mut in_target_user_agent = false;

    for line in text.lines() {
        let clean_line = line.split('#').next().unwrap_or("").trim();
        if clean_line.is_empty() {
            continue;
        }

        let lower_line = clean_line.to_lowercase();

        if lower_line.starts_with("user-agent:") {
            let agent = lower_line.trim_start_matches("user-agent:").trim();
            in_target_user_agent = agent == "*" || agent.contains("pythiasearchbot");
            continue;
        }

        if in_target_user_agent {
            if lower_line.starts_with("disallow:") {
                let path = clean_line[9..].trim().to_string();
                if !path.is_empty() {
                    disallowed.push(path);
                }
            } else if lower_line.starts_with("allow:") {
                let path = clean_line[6..].trim().to_string();
                if !path.is_empty() {
                    allowed.push(path);
                }
            } else if lower_line.starts_with("crawl-delay:") {
                if let Ok(d) = clean_line[12..].trim().parse::<u64>() {
                    delay = Some(Duration::from_secs(d));
                }
            }
        }
    }
    (disallowed, allowed, delay)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_extract_links_resolves_correctly() {
        let html = "<html><body>\
                <a href=\"/about\">About Us</a>\
                <a href=\"https://external.com/foo\">External Link</a>\
                <a href=\"javascript:void(0)\">Click Me</a>\
                <a href=\"mailto:test@test.com\">Email</a>\
                <a href=\"#section-2\">Jump to Section</a>\
            </body></html>";
        let base_url = "https://example.com/blog/article-1";
        let links = extract_links(html, base_url);

        assert_eq!(links.len(), 2);
        assert!(links.contains(&"https://example.com/about".to_string()));
        assert!(links.contains(&"https://external.com/foo".to_string()));
    }

    #[test]
    fn test_parse_robots_txt() {
        let txt = "
            # Global rules
            User-agent: *
            Disallow: /admin/
            Disallow: /private/
            Allow: /admin/public/
            Crawl-delay: 5

            # Rules for some other bot (we should ignore these)
            User-agent: Googlebot
            Disallow: /
        ";

        let (disallow, allow, delay) = parse_robots_txt(txt);

        assert_eq!(disallow, vec!["/admin/", "/private/"]);
        assert_eq!(allow, vec!["/admin/public/"]);
        assert_eq!(delay, Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_parse_robots_txt_specific_agent() {
        let txt = "
            User-agent: *
            Disallow: /everything/

            User-agent: pythiasearchbot
            Disallow: /just-secret/
            Crawl-delay: 2
        ";

        let (disallow, _allow, delay) = parse_robots_txt(txt);

        assert_eq!(disallow, vec!["/everything/", "/just-secret/"]);
        assert_eq!(delay, Some(Duration::from_secs(2)));
    }
}
