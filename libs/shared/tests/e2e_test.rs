use mockito::Server;
use reqwest::Client;
use shared::models::SearchResult;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

#[tokio::test]
async fn test_full_microservice_pipeline() {
    let mut server = Server::new_async().await;

    let _robots_mock = server
        .mock("GET", "/robots.txt")
        .with_status(200)
        .with_body("User-agent: *\nAllow: /")
        .create_async()
        .await;

    let test_url = format!("{}/test-page", server.url());
    let _page_mock = server
        .mock("GET", "/test-page")
        .with_status(200)
        .with_header("content-type", "text/html")
        .with_body("<html><body><p>Pythia microservice integration test content.</p></body></html>")
        .create_async()
        .await;

    let mut crawler = Command::new("cargo")
        .args(["run", "-p", "crawler"])
        .env("CRAWLER_SEED_URLS", &test_url)
        .stdout(Stdio::null())
        .spawn()
        .expect("Failed to start Crawler");

    let mut processor = Command::new("cargo")
        .args(["run", "-p", "processor"])
        .stdout(Stdio::null())
        .spawn()
        .expect("Failed to start Processor");

    let mut indexer = Command::new("cargo")
        .args(["run", "-p", "indexer"])
        .stdout(Stdio::null())
        .spawn()
        .expect("Failed to start Indexer");

    let mut query = Command::new("cargo")
        .args(["run", "-p", "query"])
        .stdout(Stdio::null())
        .spawn()
        .expect("Failed to start Query service");

    tokio::time::sleep(Duration::from_secs(10)).await;

    let client = Client::new();
    let query_url = "http://localhost:4000/search?q=integration";
    let mut found = false;

    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(2)).await;

        if let Ok(response) = client.get(query_url).send().await {
            if let Ok(results) = response.json::<Vec<SearchResult>>().await {
                if results
                    .iter()
                    .any(|r| r.url == test_url && r.text.contains("integration test content"))
                {
                    found = true;
                    break;
                }
            }
        }
    }

    let _ = crawler.kill().await;
    let _ = processor.kill().await;
    let _ = indexer.kill().await;
    let _ = query.kill().await;

    assert!(
        found,
        "E2E Test Failed: The test document never appeared in the search results! (This is expected until real queues are implemented)"
    );
}
