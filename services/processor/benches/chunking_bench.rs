use criterion::{Criterion, criterion_group, criterion_main};
use processor::logic::embedder::TextChunker;
use std::hint::black_box;

fn bench_text_cleaning(c: &mut Criterion) {
    let messy_text = "This   is \n\n some extremely \t\t messy text that needs   \n cleaning.";

    c.bench_function("clean_text", |b| {
        b.iter(|| {
            let _ = TextChunker::clean_text(black_box(messy_text));
        })
    });
}

fn bench_text_chunking(c: &mut Criterion) {
    let sentence = "This is a standard sentence that we will repeat to simulate a document. ";
    let document = sentence.repeat(50);

    c.bench_function("chunk_text_overlapping", |b| {
        b.iter(|| {
            let _ = TextChunker::chunk_text(black_box(&document), black_box(200), black_box(2));
        })
    });
}

criterion_group!(benches, bench_text_cleaning, bench_text_chunking);
criterion_main!(benches);
