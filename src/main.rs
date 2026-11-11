mod api;
mod job;
mod queue;
mod worker;

use api::AppState;
use queue::{JobQueue, ProcessSummary};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use worker::run_worker;

#[tokio::main]
async fn main() {
    let queue = Arc::new(Mutex::new(JobQueue::with_capacity(10)));
    let state = AppState::new(Arc::clone(&queue));

    let mut workers = JoinSet::new();

    for worker_id in 1..=3 {
        let worker_queue = Arc::clone(&queue);

        workers.spawn(async move {
            println!("worker {} started", worker_id);

            run_worker(worker_queue, move |job| async move {
                println!(
                    "worker {} running job {} ({}), retry_count {}",
                    worker_id, job.id, job.payload, job.retry_count
                );

                tokio::time::sleep(Duration::from_millis(500)).await;

                match job.payload.as_str() {
                    "flaky" if job.retry_count < 2 => Err(String::from("flaky failure")),
                    "broken" => Err(String::from("always fails")),
                    _ => Ok(()),
                }
            })
                .await
        });
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("failed to bind port 3000");

    println!("listening on http://127.0.0.1:3000");

    axum::serve(listener, api::router(state))
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to listen for Ctrl-C");
            println!("shutting down...");
        })
        .await
        .expect("server error");

    // The server has stopped taking requests. Now stop handing out jobs:
    // running jobs finish, workers exit, and waiting jobs stay in the queue.
    queue.lock().await.shutdown();

    let mut total = ProcessSummary {
        completed: 0,
        failed: 0,
    };

    while let Some(result) = workers.join_next().await {
        total.merge(result.unwrap());
    }

    let left = queue.lock().await.len();

    println!("{:?}", total);
    println!("jobs left in queue: {}", left);
}