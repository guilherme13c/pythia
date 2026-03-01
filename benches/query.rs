use criterion::{Criterion, criterion_group, criterion_main};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

fn bench_embedding(c: &mut Criterion) {
    let mut model =
        TextEmbedding::try_new(InitOptions::new(EmbeddingModel::AllMiniLML6V2)).unwrap();
    let text = vec![
        "This is a sample sentence to benchmark the embedding speed of our model.".to_string(),
    ];

    c.bench_function("generate_embedding", |b| {
        b.iter(|| {
            let _ = model.embed(text.clone(), None).unwrap();
        })
    });
}

criterion_group!(benches, bench_embedding);
criterion_main!(benches);
