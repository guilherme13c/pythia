use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn chunk_text(text: &str, chunk_size: usize) -> Vec<String> {
    text.chars()
        .collect::<Vec<char>>()
        .chunks(chunk_size)
        .map(|c| c.iter().collect::<String>())
        .collect()
}

fn bench_text_processing(c: &mut Criterion) {
    let massive_text = "Pythia search engine text processing. ".repeat(1000);

    c.bench_function("processor_chunking", |b| {
        b.iter(|| {
            let _chunks = chunk_text(black_box(&massive_text), black_box(512));
        })
    });
}

criterion_group!(benches, bench_text_processing);
criterion_main!(benches);
