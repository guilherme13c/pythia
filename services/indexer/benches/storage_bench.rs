use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use indexer::data::lancedb_store::VECTOR_DIMENSIONS;
use std::hint::black_box;
use std::sync::Arc;

fn bench_arrow_batch_creation(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("url", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                VECTOR_DIMENSIONS,
            ),
            false,
        ),
    ]));

    let url = "https://example.com";
    let chunks = vec!["Some text to index".to_string(); 10];
    let embeddings = vec![vec![0.5f32; VECTOR_DIMENSIONS as usize]; 10];

    c.bench_function("build_record_batch_10_rows", |b| {
        b.iter(|| {
            let _ = indexer::data::lancedb_store::LanceDbStore::build_record_batch(
                black_box(schema.clone()),
                black_box(url),
                black_box(Some("Title")),
                black_box(None),
                black_box(chunks.clone()),
                black_box(embeddings.clone()),
            );
        })
    });
}

criterion_group!(benches, bench_arrow_batch_creation);
criterion_main!(benches);
