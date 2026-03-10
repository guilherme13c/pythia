use scraper::{Html, Selector};
use url::Url;

pub struct ExtractedDocument {
    pub text: String,
    pub title: Option<String>,
    pub description: Option<String>,
}

pub fn parse_document(
    bytes: &[u8],
    mime_type: &str,
    url_str: &str,
) -> Result<ExtractedDocument, String> {
    if mime_type.contains("application/pdf") {
        parse_pdf(bytes, url_str)
    } else if mime_type.contains("xml") {
        let xml_str = String::from_utf8_lossy(bytes).to_string();
        Ok(parse_xml(&xml_str))
    } else {
        let html_str = String::from_utf8_lossy(bytes).to_string();
        Ok(parse_html(&html_str))
    }
}

fn parse_html(html: &str) -> ExtractedDocument {
    let document = Html::parse_document(html);

    let text = document.root_element().text().collect::<Vec<_>>().join(" ");

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

    ExtractedDocument {
        text,
        title,
        description,
    }
}

fn parse_pdf(bytes: &[u8], url_str: &str) -> Result<ExtractedDocument, String> {
    let text = pdf_extract::extract_text_from_mem(bytes)
        .map_err(|e| format!("Failed to extract PDF text: {:?}", e))?;

    let mut title = None;
    let mut description = None;

    if let Ok(doc) = lopdf::Document::load_mem(bytes) {
        extract_pdf_metadata(&doc, &mut title, &mut description);
    }

    if title.is_none() {
        title = extract_fallback_title_from_url(url_str);
    }

    Ok(ExtractedDocument {
        text,
        title,
        description,
    })
}

fn extract_pdf_metadata(
    doc: &lopdf::Document,
    title: &mut Option<String>,
    description: &mut Option<String>,
) {
    let Ok(info_ref) = doc.trailer.get(b"Info") else {
        return;
    };
    let Ok(info_dict) = doc
        .get_object(info_ref.as_reference().unwrap_or((0, 0)))
        .and_then(|obj| obj.as_dict())
    else {
        return;
    };

    if let Ok(t_bytes) = info_dict.get(b"Title").and_then(|obj| obj.as_str()) {
        let trimmed = String::from_utf8_lossy(t_bytes).trim().to_string();
        if !trimmed.is_empty() {
            *title = Some(trimmed);
        }
    }
    if let Ok(s_bytes) = info_dict.get(b"Subject").and_then(|obj| obj.as_str()) {
        let trimmed = String::from_utf8_lossy(s_bytes).trim().to_string();
        if !trimmed.is_empty() {
            *description = Some(trimmed);
        }
    }
}

fn extract_fallback_title_from_url(url_str: &str) -> Option<String> {
    let parsed_url = Url::parse(url_str).ok()?;
    let segments = parsed_url.path_segments()?;
    let last = segments.last()?;
    let fallback = last.replace(".pdf", "").replace(['-', '_'], " ");

    if !fallback.trim().is_empty() {
        Some(fallback)
    } else {
        None
    }
}

fn parse_xml(xml: &str) -> ExtractedDocument {
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
                process_xml_text(
                    &text,
                    &current_tag,
                    &mut text_content,
                    &mut title,
                    &mut description,
                );
            }
            Ok(Event::CData(e)) => {
                let text = String::from_utf8_lossy(e.as_ref());
                process_xml_text(
                    &text,
                    &current_tag,
                    &mut text_content,
                    &mut title,
                    &mut description,
                );
            }
            Ok(Event::End(_)) => current_tag.clear(),
            Ok(Event::Eof) | Err(_) => break,
            _ => (),
        }
        buf.clear();
    }

    ExtractedDocument {
        text: text_content.join(" "),
        title,
        description,
    }
}

fn process_xml_text(
    text: &str,
    current_tag: &str,
    text_content: &mut Vec<String>,
    title: &mut Option<String>,
    description: &mut Option<String>,
) {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }

    text_content.push(trimmed.to_string());

    if current_tag == "title" && title.is_none() {
        *title = Some(trimmed.to_string());
    } else if (current_tag == "description" || current_tag == "summary") && description.is_none() {
        *description = Some(trimmed.to_string());
    }
}
