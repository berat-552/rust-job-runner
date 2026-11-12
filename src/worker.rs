//! The worker loop: takes jobs from the shared queue and runs them.

use crate::job::Job;
use crate::queue::{JobQueue, ProcessSummary};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Wait before checking again when the queue is empty but still open.
const IDLE_POLL: Duration = Duration::from_millis(20);

/// Runs jobs from `queue` with `handler` until the queue is closed and empty.
///
/// `handler` receives a clone of each job. On success the job is `Completed`.
/// On failure it waits out its backoff and is requeued. Once it has no retries
/// left it is recorded as `Failed` and counted in `failed`.
///
/// The queue lock is held only to take, requeue or record a job. It is never
/// held while the handler runs or while waiting, so workers don't block each other.
///
/// Returns counts for this worker only. Combine several with `ProcessSummary::merge`.
pub(crate) async fn run_worker<F, Fut>(queue: Arc<Mutex<JobQueue>>, handler: F) -> ProcessSummary
where
    F: Fn(Job) -> Fut + Send,
    Fut: Future<Output = Result<(), String>> + Send,
{
    let mut summary = ProcessSummary {
        completed: 0,
        failed: 0,
    };

    loop {
        // Take a job and check `closed` under the same lock, so a job can't slip in between.
        let (next, closed) = {
            let mut guard = queue.lock().await;
            let next = guard.next_job();
            (next, guard.is_closed())
        };

        let Some(mut job) = next else {
            if closed {
                break;
            }

            // Queue is empty but still open: wait a moment (no lock held) and look again.
            tokio::time::sleep(IDLE_POLL).await;
            continue;
        };

        match handler(job.clone()).await {
            Ok(()) => {
                job.mark_completed().expect("job should be running");
                queue.lock().await.record_finished(&job);
                summary.completed += 1;
            }
            Err(_) => {
                job.mark_failed().expect("job should be running");

                if job.can_retry() {
                    // No lock is held here, so other workers can use the queue while we wait.
                    tokio::time::sleep(job.backoff_delay()).await;

                    if queue.lock().await.requeue(job).is_err() {
                        summary.failed += 1;
                    }
                } else {
                    queue.lock().await.record_finished(&job);
                    summary.failed += 1;
                }
            }
        }
    }

    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::JobStatus;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::task::JoinSet;

    /// A queue pre-filled with jobs and already closed, so workers exit once it drains.
    fn shared_queue_with_jobs(count: u64) -> Arc<Mutex<JobQueue>> {
        let mut queue = JobQueue::new();

        for id in 1..=count {
            queue.submit(Job::new(id, String::from("test"))).unwrap();
        }

        queue.close();

        Arc::new(Mutex::new(queue))
    }

    #[tokio::test]
    async fn on_empty_queue_returns_zero_summary() {
        let queue = shared_queue_with_jobs(0);

        let summary = run_worker(queue, |_job| async { Ok(()) }).await;

        assert_eq!(
            summary,
            ProcessSummary {
                completed: 0,
                failed: 0
            }
        );
    }

    #[tokio::test]
    async fn counts_every_job_as_completed_when_all_succeed() {
        let queue = shared_queue_with_jobs(3);

        let summary = run_worker(queue, |_job| async { Ok(()) }).await;

        assert_eq!(
            summary,
            ProcessSummary {
                completed: 3,
                failed: 0
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn job_that_always_fails_runs_four_times_then_counts_as_failed() {
        let queue = shared_queue_with_jobs(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);

        let summary = run_worker(queue, move |_job| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            async { Err(String::from("boom")) }
        })
        .await;

        assert_eq!(
            summary,
            ProcessSummary {
                completed: 0,
                failed: 1
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 4); // the original run plus 3 retries
    }

    #[tokio::test(start_paused = true)]
    async fn always_failing_job_waits_for_backoff_between_attempts() {
        let queue = shared_queue_with_jobs(1);
        let start = tokio::time::Instant::now();

        let summary = run_worker(queue, |_job| async { Err(String::from("boom")) }).await;

        assert_eq!(
            summary,
            ProcessSummary {
                completed: 0,
                failed: 1
            }
        );
        // 100 ms + 200 ms + 400 ms between the four attempts, and no wait after the last.
        assert!(start.elapsed() >= Duration::from_millis(700));
    }

    #[tokio::test]
    async fn lock_is_not_held_while_handler_runs() {
        let queue = shared_queue_with_jobs(1);
        let shared = Arc::clone(&queue);

        run_worker(queue, move |_job| {
            let shared = Arc::clone(&shared);
            async move {
                assert!(shared.try_lock().is_ok());
                Ok(())
            }
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn three_workers_process_all_jobs_between_them() {
        let queue = shared_queue_with_jobs(10);
        let mut workers = JoinSet::new();

        for _ in 0..3 {
            let worker_queue = Arc::clone(&queue);

            workers.spawn(async move { run_worker(worker_queue, |_job| async { Ok(()) }).await });
        }

        let mut total = ProcessSummary {
            completed: 0,
            failed: 0,
        };

        while let Some(result) = workers.join_next().await {
            total.merge(result.unwrap());
        }

        assert_eq!(
            total,
            ProcessSummary {
                completed: 10,
                failed: 0
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn each_job_is_processed_exactly_once_across_workers() {
        let queue = shared_queue_with_jobs(20);
        let seen: Arc<StdMutex<Vec<u64>>> = Arc::new(StdMutex::new(Vec::new()));
        let mut workers = JoinSet::new();

        for _ in 0..4 {
            let worker_queue = Arc::clone(&queue);
            let worker_seen = Arc::clone(&seen);

            workers.spawn(async move {
                run_worker(worker_queue, move |job| {
                    worker_seen.lock().unwrap().push(job.id);

                    async {
                        tokio::task::yield_now().await;
                        Ok(())
                    }
                })
                .await
            });
        }

        let mut total = ProcessSummary {
            completed: 0,
            failed: 0,
        };

        while let Some(result) = workers.join_next().await {
            total.merge(result.unwrap());
        }

        let mut ids = seen.lock().unwrap().clone();
        ids.sort();

        assert_eq!(ids, (1..=20).collect::<Vec<u64>>());
        assert_eq!(
            total,
            ProcessSummary {
                completed: 20,
                failed: 0
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn always_failing_job_is_recorded_once_per_attempt() {
        let queue = shared_queue_with_jobs(1);
        let seen: Arc<StdMutex<Vec<u64>>> = Arc::new(StdMutex::new(Vec::new()));
        let handler_seen = Arc::clone(&seen);

        let summary = run_worker(queue, move |job| {
            handler_seen.lock().unwrap().push(job.id);
            async { Err(String::from("boom")) }
        })
        .await;

        assert_eq!(
            summary,
            ProcessSummary {
                completed: 0,
                failed: 1
            }
        );
        assert_eq!(*seen.lock().unwrap(), vec![1, 1, 1, 1]); // the original run plus 3 retries
    }

    #[tokio::test(start_paused = true)]
    async fn worker_keeps_waiting_on_open_empty_queue_until_it_is_closed() {
        // Built directly (not with the helper) because this queue must stay open.
        let queue = Arc::new(Mutex::new(JobQueue::new()));
        let worker_queue = Arc::clone(&queue);

        let worker =
            tokio::spawn(async move { run_worker(worker_queue, |_job| async { Ok(()) }).await });

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!worker.is_finished());

        queue.lock().await.close();

        let summary = worker.await.unwrap();
        assert_eq!(
            summary,
            ProcessSummary {
                completed: 0,
                failed: 0
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn worker_picks_up_job_submitted_after_it_started() {
        let queue = Arc::new(Mutex::new(JobQueue::new()));
        let worker_queue = Arc::clone(&queue);

        let worker =
            tokio::spawn(async move { run_worker(worker_queue, |_job| async { Ok(()) }).await });

        tokio::time::sleep(Duration::from_millis(100)).await;

        {
            let mut guard = queue.lock().await;
            guard.submit(Job::new(1, String::from("late"))).unwrap();
            guard.close();
        }

        let summary = worker.await.unwrap();
        assert_eq!(
            summary,
            ProcessSummary {
                completed: 1,
                failed: 0
            }
        );
    }

    #[tokio::test]
    async fn succeeding_job_ends_as_completed() {
        let queue = shared_queue_with_jobs(1);

        run_worker(Arc::clone(&queue), |_job| async { Ok(()) }).await;

        assert_eq!(queue.lock().await.status_of(1), Some(JobStatus::Completed));
    }

    #[tokio::test(start_paused = true)]
    async fn always_failing_job_ends_as_failed() {
        let queue = shared_queue_with_jobs(1);

        run_worker(Arc::clone(&queue), |_job| async {
            Err(String::from("boom"))
        })
        .await;

        assert_eq!(queue.lock().await.status_of(1), Some(JobStatus::Failed));
    }

    #[tokio::test]
    async fn job_left_by_shutdown_stays_pending() {
        let queue = shared_queue_with_jobs(3);
        queue.lock().await.shutdown();

        run_worker(Arc::clone(&queue), |_job| async { Ok(()) }).await;

        assert_eq!(queue.lock().await.status_of(1), Some(JobStatus::Pending));
    }

    #[tokio::test]
    async fn worker_on_shut_down_queue_exits_without_running_jobs() {
        let queue = shared_queue_with_jobs(3);
        queue.lock().await.shutdown();

        let summary = run_worker(Arc::clone(&queue), |_job| async { Ok(()) }).await;

        assert_eq!(
            summary,
            ProcessSummary {
                completed: 0,
                failed: 0
            }
        );
        assert_eq!(queue.lock().await.len(), 3);
    }

    #[tokio::test]
    async fn worker_finishes_in_flight_job_then_stops_after_shutdown() {
        let queue = shared_queue_with_jobs(5);
        let shared = Arc::clone(&queue);

        let summary = run_worker(Arc::clone(&queue), move |_job| {
            let shared = Arc::clone(&shared);
            async move {
                // Shutdown arrives while this job is running.
                shared.lock().await.shutdown();
                Ok(())
            }
        })
        .await;

        assert_eq!(
            summary,
            ProcessSummary {
                completed: 1,
                failed: 0
            }
        );
        assert_eq!(queue.lock().await.len(), 4);
    }
}
