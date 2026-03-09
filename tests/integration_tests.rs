#[cfg(test)]
mod integration_tests {
    use futures::StreamExt;
    use lancedb::query::ExecutableQuery;
    use mockito::Server;
    use pythia::actors::crawler::manager::actor::ManagerActor;
    use pythia::actors::crawler::manager::messages::ManagerMessage;
    use pythia::actors::crawler::worker::messages::WorkerMessage;
    use pythia::actors::crawler::worker::static_worker::actor::WorkerActor;
    use pythia::actors::indexer::actor::IndexerActor;
    use pythia::actors::processor::actor::ProcessorActor;
    use ractor::Actor;
    use std::time::Duration;

    #[tokio::test]
    async fn test_full_indexing_pipeline() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .with_test_writer()
            .try_init();

        unsafe {
            std::env::set_var("FASTEMBED_CACHE_PATH", "./.models");
        }

        let shard_idx = 0;

        let _ = std::fs::create_dir_all("data");
        let _ = std::fs::remove_file(format!("data/crawler_queue_{}.db", shard_idx));
        let _ = std::fs::remove_file(format!("data/crawler_queue_{}.db-wal", shard_idx));
        let _ = std::fs::remove_file(format!("data/crawler_queue_{}.db-shm", shard_idx));
        let _ = std::fs::remove_dir_all(format!("data/pythia-vectors-{}", shard_idx));

        let mut server = Server::new_async().await;

        let _robots_mock = server
            .mock("GET", "/robots.txt")
            .with_status(200)
            .with_body("User-agent: *\nAllow: /")
            .create_async()
            .await;

        let _page_mock = server.mock("GET", "/test-page")
            .with_status(200)
            .with_header("content-type", "text/html")
            .with_body("<html><head><title>Integration Test Page</title></head><body><p>This is test content designed to be embedded and indexed by the pipeline.</p></body></html>")
            .create_async().await;

        let test_url = format!("{}/test-page", server.url());

        let (indexer_ref, _) = Actor::spawn(
            Some(format!("indexer-{}", shard_idx)),
            IndexerActor,
            shard_idx,
        )
        .await
        .unwrap();
        let (processor_ref, _) = Actor::spawn(Some("processor-0".to_string()), ProcessorActor, ())
            .await
            .unwrap();
        let (manager_ref, _) = Actor::spawn(
            Some(format!("manager-{}", shard_idx)),
            ManagerActor,
            shard_idx,
        )
        .await
        .unwrap();
        let (worker_ref, _) = Actor::spawn(
            Some(format!("worker-{}-1", shard_idx)),
            WorkerActor,
            shard_idx,
        )
        .await
        .unwrap();

        for _ in 0..50 {
            let processors_ready = !ractor::pg::get_members(&"processors".to_string()).is_empty();
            let indexers_ready = !ractor::pg::get_members(&"indexers".to_string()).is_empty();
            let managers_ready =
                !ractor::pg::get_members(&format!("manager-shard-{}", shard_idx)).is_empty();

            if processors_ready && indexers_ready && managers_ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        manager_ref
            .cast(ManagerMessage::AddUrls(vec![test_url.clone()]))
            .unwrap();

        worker_ref.cast(WorkerMessage::NoWorkAvailable).unwrap();

        let db_path = format!("data/pythia-vectors-{}", shard_idx);
        let mut found = false;

        for _ in 0..30 {
            tokio::time::sleep(Duration::from_secs(1)).await;

            if let Ok(db) = lancedb::connect(&db_path).execute().await {
                if let Ok(table) = db.open_table("search_index").execute().await {
                    if let Ok(mut stream) = table.query().execute().await {
                        while let Some(Ok(batch)) = stream.next().await {
                            let url_array = batch
                                .column_by_name("url")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<arrow::array::StringArray>()
                                .unwrap();
                            let text_array = batch
                                .column_by_name("text")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<arrow::array::StringArray>()
                                .unwrap();

                            for i in 0..batch.num_rows() {
                                if url_array.value(i) == test_url {
                                    let indexed_text = text_array.value(i);
                                    if indexed_text
                                        .contains("This is test content designed to be embedded")
                                    {
                                        found = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if found {
                break;
            }
        }

        assert!(
            found,
            "The test URL was not successfully indexed into LanceDB within the timeout!"
        );

        worker_ref.stop(None);
        manager_ref.stop(None);
        processor_ref.stop(None);
        indexer_ref.stop(None);
    }
}
