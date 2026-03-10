use scraper::{Html, Selector};
use std::sync::OnceLock;
use std::time::Duration;
use url::Url;

fn get_link_selector() -> &'static Selector {
    static SELECTOR: OnceLock<Selector> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("a[href]").expect("Failed to parse CSS selector"))
}

pub fn extract_links(bytes: &[u8], mime_type: &str, base_url_str: &str) -> Vec<String> {
    let mut links = if mime_type.contains("application/pdf") {
        extract_links_from_pdf(bytes, base_url_str)
    } else if mime_type.contains("xml") {
        let xml_str = String::from_utf8_lossy(bytes);
        extract_links_from_xml(&xml_str, base_url_str)
    } else {
        let html_str = String::from_utf8_lossy(bytes);
        extract_links_from_html(&html_str, base_url_str)
    };

    links.sort();
    links.dedup();

    links
}

fn extract_links_from_html(html: &str, base_url_str: &str) -> Vec<String> {
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
    links
}

fn extract_links_from_xml(xml: &str, base_url_str: &str) -> Vec<String> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut links = Vec::new();
    let mut current_tag = String::new();
    let mut buf = Vec::new();

    if let Ok(mut base_url) = Url::parse(base_url_str) {
        base_url.set_fragment(None);

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    current_tag = String::from_utf8_lossy(e.name().as_ref()).to_lowercase();
                    if current_tag == "link" {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"href" {
                                if let Ok(val) = std::str::from_utf8(attr.value.as_ref()) {
                                    if let Ok(mut abs_url) = base_url.join(val) {
                                        abs_url.set_fragment(None);
                                        if abs_url.scheme() == "http" || abs_url.scheme() == "https"
                                        {
                                            links.push(abs_url.to_string());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(Event::Text(e)) => {
                    if current_tag == "loc" || current_tag == "link" {
                        if let Ok(val) = std::str::from_utf8(e.as_ref()) {
                            if let Ok(mut abs_url) = base_url.join(val.trim()) {
                                abs_url.set_fragment(None);
                                if abs_url.scheme() == "http" || abs_url.scheme() == "https" {
                                    links.push(abs_url.to_string());
                                }
                            }
                        }
                    }
                }
                Ok(Event::End(_)) => current_tag.clear(),
                Ok(Event::Eof) | Err(_) => break,
                _ => (),
            }
            buf.clear();
        }
    }
    links
}

fn extract_links_from_pdf(bytes: &[u8], base_url_str: &str) -> Vec<String> {
    let mut links = Vec::new();

    if let Ok(mut base_url) = Url::parse(base_url_str) {
        base_url.set_fragment(None);

        if let Ok(doc) = lopdf::Document::load_mem(bytes) {
            for page_id in doc.get_pages().values() {
                if let Ok(page_dict) = doc.get_object(*page_id).and_then(|obj| obj.as_dict()) {
                    if let Ok(annots) = page_dict.get(b"Annots").and_then(|obj| obj.as_array()) {
                        for annot in annots {
                            if let Ok(annot_dict) = doc
                                .get_object(annot.as_reference().unwrap_or((0, 0)))
                                .and_then(|obj| obj.as_dict())
                            {
                                if let Ok(subtype) =
                                    annot_dict.get(b"Subtype").and_then(|obj| obj.as_name())
                                {
                                    if subtype == b"Link" {
                                        if let Ok(action_dict) =
                                            annot_dict.get(b"A").and_then(|obj| obj.as_dict())
                                        {
                                            if let Ok(uri) =
                                                action_dict.get(b"URI").and_then(|obj| obj.as_str())
                                            {
                                                if let Ok(val) = std::str::from_utf8(uri) {
                                                    if let Ok(mut abs_url) = base_url.join(val) {
                                                        abs_url.set_fragment(None);
                                                        if abs_url.scheme() == "http"
                                                            || abs_url.scheme() == "https"
                                                        {
                                                            links.push(abs_url.to_string());
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
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

        let links = extract_links(html.as_bytes(), "text/html", base_url);

        assert_eq!(links.len(), 2);
        assert!(links.contains(&"https://example.com/about".to_string()));
        assert!(links.contains(&"https://external.com/foo".to_string()));
    }

    #[test]
    fn test_extract_links_from_xml_sitemap() {
        let xml = r#"
            <?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url>
                    <loc>https://example.com/page1</loc>
                </url>
                <url>
                    <loc>/page2</loc>
                </url>
            </urlset>
        "#;
        let base_url = "https://example.com/sitemap.xml";

        let links = extract_links(xml.as_bytes(), "application/xml", base_url);

        assert_eq!(links.len(), 2);
        assert!(links.contains(&"https://example.com/page1".to_string()));
        assert!(links.contains(&"https://example.com/page2".to_string()));
    }

    #[test]
    fn test_extract_links_from_xml_rss() {
        let xml = r#"
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
                <channel>
                    <link>https://example.com/blog</link>
                    <item>
                        <link>https://example.com/blog/post-1</link>
                    </item>
                </channel>
            </rss>
        "#;
        let base_url = "https://example.com/feed.xml";

        let links = extract_links(xml.as_bytes(), "text/xml", base_url);

        assert_eq!(links.len(), 2);
        assert!(links.contains(&"https://example.com/blog".to_string()));
        assert!(links.contains(&"https://example.com/blog/post-1".to_string()));
    }

    #[test]
    fn test_extract_links_from_pdf() {
        use lopdf::{Dictionary, Document, Object, StringFormat};

        let mut doc = Document::with_version("1.5");
        let pages_id = doc.new_object_id();

        let mut action_dict = Dictionary::new();
        action_dict.set("Type", Object::Name(b"Action".to_vec()));
        action_dict.set("S", Object::Name(b"URI".to_vec()));
        action_dict.set(
            "URI",
            Object::String(
                b"https://external.com/from-pdf".to_vec(),
                StringFormat::Literal,
            ),
        );

        let mut annot_dict = Dictionary::new();
        annot_dict.set("Type", Object::Name(b"Annot".to_vec()));
        annot_dict.set("Subtype", Object::Name(b"Link".to_vec()));
        annot_dict.set("A", Object::Dictionary(action_dict));
        let annot_id = doc.add_object(annot_dict);

        let mut page_dict = Dictionary::new();
        page_dict.set("Type", Object::Name(b"Page".to_vec()));
        page_dict.set("Parent", Object::Reference(pages_id));
        page_dict.set("Annots", Object::Array(vec![Object::Reference(annot_id)]));
        let page_id = doc.add_object(page_dict);

        let mut pages_dict = Dictionary::new();
        pages_dict.set("Type", Object::Name(b"Pages".to_vec()));
        pages_dict.set("Kids", Object::Array(vec![Object::Reference(page_id)]));
        pages_dict.set("Count", Object::Integer(1));
        doc.objects.insert(pages_id, Object::Dictionary(pages_dict));

        let mut catalog = Dictionary::new();
        catalog.set("Type", Object::Name(b"Catalog".to_vec()));
        catalog.set("Pages", Object::Reference(pages_id));
        let catalog_id = doc.add_object(catalog);

        doc.trailer.set("Root", Object::Reference(catalog_id));

        let mut pdf_bytes = Vec::new();
        doc.save_to(&mut pdf_bytes)
            .expect("Failed to save test PDF");

        let base_url = "https://example.com/document.pdf";
        let links = extract_links(&pdf_bytes, "application/pdf", base_url);

        assert_eq!(links.len(), 1);
        assert!(links.contains(&"https://external.com/from-pdf".to_string()));
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
