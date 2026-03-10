use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn bench_url_formatting(c: &mut Criterion) {
    let base = "http://indexer-service:3002";

    c.bench_function("query_url_format", |b| {
        b.iter(|| {
            let _ = black_box(format!("{}/search", base));
        })
    });
}

fn bench_json_payload_construction(c: &mut Criterion) {
    let vector = vec![0.1f32; 384];
    let limit = 10;

    c.bench_function("search_payload_serialization", |b| {
        b.iter(|| {
            let _ = black_box(serde_json::json!({
                "vector": vector,
                "limit": limit
            }));
        })
    });
}

criterion_group!(
    benches,
    bench_url_formatting,
    bench_json_payload_construction
);
criterion_main!(benches);
