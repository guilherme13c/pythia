use ractor::Actor;
use std::sync::Arc;
use tokio::time::Duration;

pub mod communication;
pub mod data;
pub mod logic;

use communication::publisher::MockPublisher;
use data::blob_storage::MockBlobStorage;
use data::frontier::ManagerState;
use logic::manager::{ManagerActor, ManagerMessage};

use headless_chrome::{Browser, LaunchOptions};
use logic::worker::{WorkerActor, WorkerState, WorkerType};

#[tokio::main]
async fn main() {
    println!("🚀 Bootstrapping Crawler Service...");

    let blob_storage = Arc::new(MockBlobStorage);
    let publisher = Arc::new(MockPublisher);

    let manager_state = ManagerState::with_db(":memory:", 10_000, 0.01);

    let (manager_ref, _) = Actor::spawn(Some("manager".to_string()), ManagerActor, manager_state)
        .await
        .unwrap();

    for i in 1..=3 {
        let worker_name = format!("worker-{}", i);

        let worker_type = if i == 1 {
            let browser = Browser::new(
                LaunchOptions::default_builder()
                    .headless(true)
                    .build()
                    .expect("Failed to build LaunchOptions"),
            )
            .expect("Failed to launch headless chrome");
            WorkerType::Dynamic(Arc::new(browser))
        } else {
            WorkerType::Static
        };

        let worker_state = WorkerState {
            http_client: reqwest::Client::new(),
            blob_storage: blob_storage.clone(),
            publisher: publisher.clone(),
            worker_type,
        };

        Actor::spawn(Some(worker_name.clone()), WorkerActor, worker_state)
            .await
            .unwrap();

        let _ = manager_ref.cast(ManagerMessage::RequestWork(worker_name));
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    let _ = manager_ref.cast(ManagerMessage::AddUrls(vec![
        "https://www.rust-lang.org/".to_string(),
        "https://tokio.rs/".to_string(),
    ]));

    tokio::signal::ctrl_c().await.unwrap();
    println!("Shutting down Crawler Service...");
}
