use pythia::actors::crawler::manager::actor::ManagerActor;
use pythia::actors::crawler::manager::state::{DomainMetadata, ManagerState};
use pythia::actors::crawler::worker::messages::WorkerMessage;

use criterion::{Criterion, criterion_group, criterion_main};
use pprof::criterion::{Output, PProfProfiler};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use scraper::{Html, Selector};
use std::hint::black_box;
use tokio::time::Instant;

struct DummyWorker;
impl Actor for DummyWorker {
    type Msg = WorkerMessage;
    type State = ();
    type Arguments = ();
    async fn pre_start(&self, _: ActorRef<Self::Msg>, _: ()) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
    async fn handle(
        &self,
        _: ActorRef<Self::Msg>,
        _: Self::Msg,
        _: &mut (),
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}

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

fn bench_frontier_ingestion(c: &mut Criterion) {
    let mut group = c.benchmark_group("frontier_management");
    let manager = ManagerActor;

    for batch_size in [100, 500, 1000].iter() {
        group.bench_with_input(
            format!("ingest_batch_{}", batch_size),
            batch_size,
            |b, &size| {
                b.iter(|| {
                    let mut state = ManagerState::in_memory();
                    let urls: Vec<String> = (0..size)
                        .map(|i| format!("https://example.com/page_{}", i))
                        .collect();

                    manager.handle_add_urls(&mut state, urls);
                })
            },
        );
    }
    group.finish();
}

fn bench_scheduler_efficiency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let manager = ManagerActor;

    let (_worker_ref, _) = rt.block_on(async {
        Actor::spawn(Some("dummy-worker".to_string()), DummyWorker, ())
            .await
            .unwrap()
    });

    let mut state = ManagerState::in_memory();
    let domain = "wikipedia.org";
    let mut metadata = DomainMetadata::default_unfetched();
    metadata.rules_fetched = true;
    metadata.last_hit = Some(Instant::now());
    state.domain_metadata.insert(domain.to_string(), metadata);

    for i in 0..1000 {
        state
            .static_frontier
            .push_back(format!("https://{}/page_{}", domain, i));
    }
    state
        .static_frontier
        .push_back("https://available-domain.com/start".to_string());

    c.bench_function("scheduler_skip_delayed_urls", |b| {
        b.iter(|| {
            manager.handle_request_work(black_box(&mut state), "dummy-worker".to_string());
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_html_parsing, bench_frontier_ingestion, bench_scheduler_efficiency
}
criterion_main!(benches);
