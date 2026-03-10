use crawler::logic::extract::{extract_links, parse_robots_txt};
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn bench_html_extraction(c: &mut Criterion) {
    let html = r#"
        <html><body>
            <a href="/link1">One</a>
            <a href="https://external.com/link2">Two</a>
            <div>Lots of other content...</div>
            <a href="/link3">Three</a>
        </body></html>
    "#;
    let base_url = "https://example.com/page";

    c.bench_function("extract_links_html", |b| {
        b.iter(|| {
            let _ = extract_links(
                black_box(html.as_bytes()),
                black_box("text/html"),
                black_box(base_url),
            );
        })
    });
}

fn bench_robots_txt_parsing(c: &mut Criterion) {
    let robots_txt = "
        User-agent: *
        Disallow: /admin/
        Disallow: /private/
        Allow: /public/
        Crawl-delay: 5
        
        User-agent: pythiasearchbot
        Disallow: /secret/
    ";

    c.bench_function("parse_robots_txt", |b| {
        b.iter(|| {
            let _ = parse_robots_txt(black_box(robots_txt));
        })
    });
}

criterion_group!(benches, bench_html_extraction, bench_robots_txt_parsing);
criterion_main!(benches);
