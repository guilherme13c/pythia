use criterion::{Criterion, criterion_group, criterion_main};
use scraper::{Html, Selector};
use std::hint::black_box;

fn bench_html_parsing(c: &mut Criterion) {
    let html_content = r#"
        <!DOCTYPE html>
        <html>
        <head><title>Test Page</title></head>
        <body>
            <h1>Pythia Engine</h1>
            <p>This is a <strong>benchmark</strong> test with <a href="/next">a link</a>.</p>
            <div>
                <a href="https://example.com/1">Link 1</a>
                <a href="https://example.com/2">Link 2</a>
            </div>
        </body>
        </html>
    "#;

    let a_selector = Selector::parse("a").unwrap();
    let body_selector = Selector::parse("body").unwrap();

    c.bench_function("crawler_html_processing", |b| {
        b.iter(|| {
            let document = Html::parse_document(black_box(html_content));

            let _text: String = document
                .select(&body_selector)
                .flat_map(|el| el.text())
                .collect::<Vec<_>>()
                .join(" ");

            let _links: Vec<_> = document
                .select(&a_selector)
                .filter_map(|el| el.value().attr("href"))
                .collect();
        })
    });
}

criterion_group!(benches, bench_html_parsing);
criterion_main!(benches);
