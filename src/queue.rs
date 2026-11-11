use crate::job::{Job, JobStatus};
use std::collections::{HashMap, VecDeque};

#[derive(Debug, PartialEq)]
pub(crate) enum QueueError {
    JobNotPending,
    RetryNotAllowed,
    QueueFull,
}

#[derive(Debug, PartialEq)]
pub(crate) struct ProcessSummary {
    pub completed: usize,
    pub failed: usize,
}

impl ProcessSummary {
    pub fn merge(&mut self, other: ProcessSummary) {
        self.completed += other.completed;
        self.failed += other.failed;
    }
}

pub(crate) struct JobQueue {
    jobs: VecDeque<Job>,
    closed: bool,
    stopped: bool,
    capacity: Option<usize>,
    statuses: HashMap<u64, JobStatus>,
}

impl JobQueue {
    pub fn new() -> JobQueue {
        JobQueue {
            jobs: VecDeque::new(),
            closed: false,
            stopped: false,
            capacity: None,
            statuses: HashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> JobQueue {
        JobQueue {
            capacity: Some(capacity),
            ..JobQueue::new()
        }
    }

    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Stop handing out jobs. Jobs still waiting stay in the queue.
    pub fn shutdown(&mut self) {
        self.stopped = true;
        self.closed = true;
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn status_of(&self, id: u64) -> Option<JobStatus> {
        self.statuses.get(&id).copied()
    }

    /// Store a job's current status. Used when a job reaches a final state.
    pub fn record_finished(&mut self, job: &Job) {
        self.statuses.insert(job.id, job.status());
    }

    pub fn submit(&mut self, job: Job) -> Result<(), QueueError> {
        if job.status() != JobStatus::Pending {
            return Err(QueueError::JobNotPending);
        }

        if let Some(capacity) = self.capacity {
            if self.jobs.len() >= capacity {
                return Err(QueueError::QueueFull);
            }
        }

        self.statuses.insert(job.id, JobStatus::Pending);
        self.jobs.push_back(job);
        Ok(())
    }

    pub fn next_job(&mut self) -> Option<Job> {
        if self.stopped {
            return None;
        }

        let mut job = self.jobs.pop_front()?;
        job.mark_running()
            .expect("queued jobs should be pending");
        self.statuses.insert(job.id, JobStatus::Running);
        Some(job)
    }

    pub async fn process_next<F, Fut>(&mut self, handler: F) -> Option<Job>
    where
        F: Fn(Job) -> Fut,
        Fut: Future<Output = Result<(), String>>,
    {
        let mut job = self.next_job()?;

        match handler(job.clone()).await {
            Ok(()) => job.mark_completed().expect("job should be running"),
            Err(_) => job.mark_failed().expect("job should be running"),
        }

        Some(job)
    }

    pub fn requeue(&mut self, mut job: Job) -> Result<(), QueueError> {
        job.retry().map_err(|_| QueueError::RetryNotAllowed)?;

        // Retries bypass the capacity limit: the job was already accepted, so it isn't new load.
        self.statuses.insert(job.id, JobStatus::Pending);
        self.jobs.push_back(job);
        Ok(())
    }

    pub async fn process_all<F, Fut>(&mut self, handler: F) -> ProcessSummary
    where
        F: Fn(Job) -> Fut,
        Fut: Future<Output = Result<(), String>>,
    {
        let mut summary = ProcessSummary {
            completed: 0,
            failed: 0,
        };

        while let Some(job) = self.process_next(&handler).await {
            match job.status() {
                JobStatus::Completed => summary.completed += 1,
                _ => {
                    // The job failed. If it can't be retried, it's permanently failed.
                    if self.requeue(job).is_err() {
                        summary.failed += 1;
                    }
                }
            }
        }

        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue_with_jobs(count: u64) -> JobQueue {
        let mut queue = JobQueue::new();

        for id in 1..=count {
            queue
                .submit(Job::new(id, String::from("test")))
                .unwrap();
        }

        queue
    }

    mod new {
        use super::*;

        #[test]
        fn queue_is_empty() {
            let queue = JobQueue::new();

            assert!(queue.is_empty());
        }

        #[test]
        fn queue_has_length_of_zero() {
            let queue = JobQueue::new();

            assert_eq!(queue.len(), 0);
        }
    }

    mod submit {
        use super::*;

        #[test]
        fn one_job_gives_length_of_one() {
            let mut queue = JobQueue::new();
            queue
                .submit(Job::new(1, String::from("test")))
                .unwrap();

            assert_eq!(queue.len(), 1);
        }

        #[test]
        fn three_jobs_gives_length_of_three() {
            let queue = queue_with_jobs(3);

            assert_eq!(queue.len(), 3);
        }

        #[test]
        fn running_job_is_rejected_and_queue_stays_empty() {
            let mut queue = JobQueue::new();
            let mut job = Job::new(1, String::from("test"));
            job.mark_running().unwrap();

            let result = queue.submit(job);

            assert_eq!(result, Err(QueueError::JobNotPending));
            assert_eq!(queue.len(), 0);
        }

        #[test]
        fn failed_job_is_rejected() {
            let mut queue = JobQueue::new();
            let mut job = Job::new(1, String::from("test"));
            job.mark_running().unwrap();
            job.mark_failed().unwrap();

            let result = queue.submit(job);

            assert_eq!(result, Err(QueueError::JobNotPending));
            assert_eq!(queue.len(), 0);
        }
    }

    mod next_job {
        use super::*;

        #[test]
        fn on_empty_queue_returns_none() {
            let mut queue = JobQueue::new();

            assert!(queue.next_job().is_none());
        }

        #[test]
        fn returns_submitted_job_and_empties_queue() {
            let mut queue = JobQueue::new();
            queue
                .submit(Job::new(1, String::from("test")))
                .unwrap();

            let result = queue.next_job();

            assert!(result.is_some());
            assert_eq!(result.unwrap().id, 1);
            assert_eq!(queue.len(), 0);
        }

        #[test]
        fn returns_jobs_in_fifo_order() {
            let mut queue = queue_with_jobs(3);

            assert_eq!(queue.next_job().unwrap().id, 1);
            assert_eq!(queue.next_job().unwrap().id, 2);
            assert_eq!(queue.next_job().unwrap().id, 3);
            assert!(queue.next_job().is_none());
        }

        #[test]
        fn reduces_length_from_three_to_two() {
            let mut queue = queue_with_jobs(3);

            queue.next_job();

            assert_eq!(queue.len(), 2);
        }

        #[test]
        fn marks_returned_job_as_running() {
            let mut queue = queue_with_jobs(1);

            let job = queue.next_job().unwrap();

            assert_eq!(job.status(), JobStatus::Running);
        }

        #[test]
        fn leaves_remaining_jobs_pending() {
            let mut queue = queue_with_jobs(2);

            queue.next_job();

            assert_eq!(queue.jobs.front().unwrap().status(), JobStatus::Pending);
        }
    }

    mod is_empty {
        use super::*;

        #[test]
        fn false_after_submit() {
            let mut queue = JobQueue::new();
            queue
                .submit(Job::new(1, String::from("test")))
                .unwrap();

            assert!(!queue.is_empty());
        }

        #[test]
        fn true_after_last_job_is_taken() {
            let mut queue = JobQueue::new();
            queue
                .submit(Job::new(1, String::from("test")))
                .unwrap();

            queue.next_job();

            assert!(queue.is_empty());
        }
    }

    mod process_next {
        use super::*;

        #[tokio::test]
        async fn on_empty_queue_returns_none() {
            let mut queue = JobQueue::new();

            assert!(queue
                .process_next(|_job| async { Ok(()) })
                .await
                .is_none());
        }

        #[tokio::test]
        async fn marks_job_completed_when_handler_succeeds() {
            let mut queue = queue_with_jobs(1);

            let job = queue
                .process_next(|_job| async { Ok(()) })
                .await
                .unwrap();

            assert_eq!(job.status(), JobStatus::Completed);
        }

        #[tokio::test]
        async fn marks_job_failed_when_handler_fails() {
            let mut queue = queue_with_jobs(1);

            let job = queue
                .process_next(|_job| async { Err(String::from("boom")) })
                .await
                .unwrap();

            assert_eq!(job.status(), JobStatus::Failed);
        }

        #[tokio::test]
        async fn removes_job_from_queue() {
            let mut queue = queue_with_jobs(2);

            queue.process_next(|_job| async { Ok(()) }).await;

            assert_eq!(queue.len(), 1);
        }

        #[tokio::test]
        async fn handler_receives_running_job() {
            let mut queue = queue_with_jobs(1);

            queue
                .process_next(|job| {
                    assert_eq!(job.status(), JobStatus::Running);
                    async { Ok(()) }
                })
                .await;
        }
    }

    mod requeue {
        use super::*;

        #[tokio::test]
        async fn failed_job_under_limit_goes_back_to_queue_as_pending() {
            let mut queue = queue_with_jobs(1);
            let job = queue
                .process_next(|_job| async { Err(String::from("boom")) })
                .await
                .unwrap();

            let result = queue.requeue(job);

            assert_eq!(result, Ok(()));
            assert_eq!(queue.len(), 1);

            let requeued = queue.jobs.back().unwrap();
            assert_eq!(requeued.status(), JobStatus::Pending);
            assert_eq!(requeued.retry_count, 1);
        }

        #[tokio::test]
        async fn failed_job_at_retry_limit_is_rejected() {
            let mut queue = queue_with_jobs(1);

            // The retry limit is 3 (MAX_RETRIES in job.rs, which is private).
            for _ in 0..3 {
                let job = queue
                    .process_next(|_job| async { Err(String::from("boom")) })
                    .await
                    .unwrap();
                queue.requeue(job).unwrap();
            }

            let job = queue
                .process_next(|_job| async { Err(String::from("boom")) })
                .await
                .unwrap();

            let result = queue.requeue(job);

            assert_eq!(result, Err(QueueError::RetryNotAllowed));
            assert_eq!(queue.len(), 0);
        }

        #[test]
        fn job_that_has_not_failed_is_rejected() {
            let mut queue = JobQueue::new();
            let job = Job::new(1, String::from("test"));

            let result = queue.requeue(job);

            assert_eq!(result, Err(QueueError::RetryNotAllowed));
            assert_eq!(queue.len(), 0);
        }
    }

    mod process_all {
        use super::*;
        use std::cell::Cell;

        #[tokio::test]
        async fn on_empty_queue_returns_zero_summary() {
            let mut queue = JobQueue::new();

            let summary = queue.process_all(|_job| async { Ok(()) }).await;

            assert_eq!(summary, ProcessSummary { completed: 0, failed: 0 });
        }

        #[tokio::test]
        async fn counts_every_job_as_completed_when_all_succeed() {
            let mut queue = queue_with_jobs(3);

            let summary = queue.process_all(|_job| async { Ok(()) }).await;

            assert_eq!(summary, ProcessSummary { completed: 3, failed: 0 });
            assert!(queue.is_empty());
        }

        #[tokio::test]
        async fn retries_job_that_fails_twice_then_succeeds() {
            let mut queue = queue_with_jobs(1);

            let summary = queue
                .process_all(|job| async move {
                    if job.retry_count < 2 {
                        Err(String::from("boom"))
                    } else {
                        Ok(())
                    }
                })
                .await;

            assert_eq!(summary, ProcessSummary { completed: 1, failed: 0 });
            assert!(queue.is_empty());
        }

        #[tokio::test]
        async fn job_that_always_fails_runs_four_times_then_counts_as_failed() {
            let mut queue = queue_with_jobs(1);
            let calls = Cell::new(0);

            let summary = queue
                .process_all(|_job| {
                    calls.set(calls.get() + 1);
                    async { Err(String::from("boom")) }
                })
                .await;

            assert_eq!(summary, ProcessSummary { completed: 0, failed: 1 });
            assert_eq!(calls.get(), 4); // the original run plus 3 retries
            assert!(queue.is_empty());
        }
    }

    mod process_summary {
        use super::*;

        #[test]
        fn merge_adds_both_counts() {
            let mut total = ProcessSummary { completed: 3, failed: 0 };

            total.merge(ProcessSummary { completed: 2, failed: 1 });

            assert_eq!(total, ProcessSummary { completed: 5, failed: 1 });
        }
    }

    mod close {
        use super::*;

        #[test]
        fn new_queue_is_not_closed() {
            let queue = JobQueue::new();

            assert!(!queue.is_closed());
        }

        #[test]
        fn close_marks_queue_as_closed() {
            let mut queue = JobQueue::new();

            queue.close();

            assert!(queue.is_closed());
        }
    }

    mod shutdown {
        use super::*;

        #[test]
        fn next_job_returns_none_and_keeps_remaining_jobs() {
            let mut queue = queue_with_jobs(3);

            queue.shutdown();

            assert!(queue.next_job().is_none());
            assert_eq!(queue.len(), 3);
        }

        #[test]
        fn also_closes_the_queue() {
            let mut queue = JobQueue::new();

            queue.shutdown();

            assert!(queue.is_closed());
        }
    }

    mod capacity {
        use super::*;

        #[test]
        fn bounded_queue_accepts_jobs_up_to_capacity() {
            let mut queue = JobQueue::with_capacity(3);

            for id in 1..=3 {
                queue
                    .submit(Job::new(id, String::from("test")))
                    .unwrap();
            }

            assert_eq!(queue.len(), 3);
        }

        #[test]
        fn submit_beyond_capacity_returns_queue_full_and_keeps_length() {
            let mut queue = JobQueue::with_capacity(2);

            for id in 1..=2 {
                queue
                    .submit(Job::new(id, String::from("test")))
                    .unwrap();
            }

            let result = queue.submit(Job::new(3, String::from("test")));

            assert_eq!(result, Err(QueueError::QueueFull));
            assert_eq!(queue.len(), 2);
        }

        #[test]
        fn submit_works_again_after_a_job_is_taken() {
            let mut queue = JobQueue::with_capacity(1);
            queue
                .submit(Job::new(1, String::from("test")))
                .unwrap();

            queue.next_job();

            let result = queue.submit(Job::new(2, String::from("test")));

            assert_eq!(result, Ok(()));
            assert_eq!(queue.len(), 1);
        }

        #[tokio::test]
        async fn requeue_is_not_blocked_by_a_full_queue() {
            let mut queue = JobQueue::with_capacity(1);
            queue
                .submit(Job::new(1, String::from("test")))
                .unwrap();

            // Take job 1 and fail it, which empties the queue.
            let failed = queue
                .process_next(|_job| async { Err(String::from("boom")) })
                .await
                .unwrap();

            // Fill the queue again, then try to retry the failed job.
            queue
                .submit(Job::new(2, String::from("test")))
                .unwrap();

            let result = queue.requeue(failed);

            assert_eq!(result, Ok(()));
            assert_eq!(queue.len(), 2); // deliberately over capacity
        }

        #[test]
        fn unbounded_queue_accepts_many_jobs() {
            let queue = queue_with_jobs(1000);

            assert_eq!(queue.len(), 1000);
        }
    }

    mod status {
        use super::*;

        #[test]
        fn unknown_id_returns_none() {
            let queue = JobQueue::new();

            assert_eq!(queue.status_of(99), None);
        }

        #[test]
        fn submit_records_pending() {
            let queue = queue_with_jobs(1);

            assert_eq!(queue.status_of(1), Some(JobStatus::Pending));
        }

        #[test]
        fn next_job_records_running() {
            let mut queue = queue_with_jobs(1);

            queue.next_job();

            assert_eq!(queue.status_of(1), Some(JobStatus::Running));
        }

        #[test]
        fn record_finished_stores_completed() {
            let mut queue = JobQueue::new();
            let mut job = Job::new(1, String::from("test"));
            job.mark_running().unwrap();
            job.mark_completed().unwrap();

            queue.record_finished(&job);

            assert_eq!(queue.status_of(1), Some(JobStatus::Completed));
        }

        #[test]
        fn record_finished_stores_failed() {
            let mut queue = JobQueue::new();
            let mut job = Job::new(1, String::from("test"));
            job.mark_running().unwrap();
            job.mark_failed().unwrap();

            queue.record_finished(&job);

            assert_eq!(queue.status_of(1), Some(JobStatus::Failed));
        }

        #[tokio::test]
        async fn requeue_records_pending() {
            let mut queue = queue_with_jobs(1);
            let failed = queue
                .process_next(|_job| async { Err(String::from("boom")) })
                .await
                .unwrap();

            queue.requeue(failed).unwrap();

            assert_eq!(queue.status_of(1), Some(JobStatus::Pending));
        }
    }
}