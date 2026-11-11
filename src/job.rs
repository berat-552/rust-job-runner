//! A single unit of work and the rules for changing its status.

use std::time::Duration;

/// Retries allowed after the first attempt, so a job runs at most 4 times.
const MAX_RETRIES: u32 = 3;
/// Wait before the first retry. Doubles with each further retry.
const BASE_BACKOFF: Duration = Duration::from_millis(100);
/// Upper limit on the wait between retries.
const MAX_BACKOFF: Duration = Duration::from_secs(2);

/// A unit of work. `status` is private and only changes through the methods
/// below, so an invalid status change can't happen.
#[derive(Clone)]
pub(crate) struct Job {
    pub id: u64,
    /// Input for the handler.
    pub payload: String,
    status: JobStatus,
    /// Retries so far. 0 on the first attempt.
    pub retry_count: u32,
}

impl Job {
    /// Creates a `Pending` job with no retries.
    pub(crate) fn new(id: u64, payload: String) -> Self {
        Self {
            id,
            payload,
            status: JobStatus::default(),
            retry_count: 0,
        }
    }

    pub(crate) fn status(&self) -> JobStatus {
        self.status
    }

    /// `Pending` -> `Running`. Any other status returns `InvalidTransition`.
    pub(crate) fn mark_running(&mut self) -> Result<(), JobError> {
        match self.status {
            JobStatus::Pending => {
                self.status = JobStatus::Running;
                Ok(())
            }
            _ => Err(JobError::InvalidTransition),
        }
    }

    /// `Running` -> `Completed`. Any other status returns `InvalidTransition`.
    pub(crate) fn mark_completed(&mut self) -> Result<(), JobError> {
        match self.status {
            JobStatus::Running => {
                self.status = JobStatus::Completed;
                Ok(())
            }
            _ => Err(JobError::InvalidTransition),
        }
    }

    /// `Running` -> `Failed`. Any other status returns `InvalidTransition`.
    pub(crate) fn mark_failed(&mut self) -> Result<(), JobError> {
        match self.status {
            JobStatus::Running => {
                self.status = JobStatus::Failed;
                Ok(())
            }
            _ => Err(JobError::InvalidTransition),
        }
    }

    /// `Failed` -> `Pending`, and adds 1 to `retry_count`.
    ///
    /// Returns `RetryLimitReached` once `retry_count` hits `MAX_RETRIES`, and
    /// `InvalidTransition` if the job isn't `Failed`. Nothing changes on error.
    pub(crate) fn retry(&mut self) -> Result<(), JobError> {
        match self.status {
            JobStatus::Failed => {
                if self.retry_count >= MAX_RETRIES {
                    return Err(JobError::RetryLimitReached);
                }

                self.status = JobStatus::Pending;
                self.retry_count += 1;
                Ok(())
            }
            _ => Err(JobError::InvalidTransition),
        }
    }

    /// Wait before the next retry: 100 ms, 200 ms, 400 ms, ... capped at `MAX_BACKOFF`.
    ///
    /// Uses saturating maths, so a large `retry_count` can't overflow.
    pub(crate) fn backoff_delay(&self) -> Duration {
        let factor = 2u32.saturating_pow(self.retry_count);

        BASE_BACKOFF.saturating_mul(factor).min(MAX_BACKOFF)
    }

    /// True if the job is `Failed` and has retries left. A worker uses this to
    /// decide between waiting and requeueing, or giving up.
    pub(crate) fn can_retry(&self) -> bool {
        self.status == JobStatus::Failed && self.retry_count < MAX_RETRIES
    }
}

/// Where a job is: `Pending` -> `Running` -> `Completed` or `Failed`.
/// A failed job goes back to `Pending` through `Job::retry`.
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub(crate) enum JobStatus {
    #[default]
    Pending,
    Running,
    Completed,
    Failed,
}

/// Why a status change was refused.
#[derive(Debug, PartialEq)]
pub(crate) enum JobError {
    /// The job's current status doesn't allow this change.
    InvalidTransition,
    /// The job has already been retried `MAX_RETRIES` times.
    RetryLimitReached,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns a `Failed` job that has already been retried `count` times.
    fn failed_job_with_retries(count: u32) -> Job {
        let mut job = Job::new(1, "Generate job".to_string());

        for _ in 0..count {
            job.mark_running().unwrap();
            job.mark_failed().unwrap();
            job.retry().unwrap();
        }

        job.mark_running().unwrap();
        job.mark_failed().unwrap();

        job
    }

    #[test]
    fn new_job_has_correct_id_and_payload() {
        let job = Job::new(1, "Generate job".to_string());

        assert_eq!(job.id, 1);
        assert_eq!(job.payload, "Generate job");
    }

    #[test]
    fn new_job_starts_as_pending() {
        let job = Job::new(1, "Generate job".to_string());

        assert_eq!(job.status(), JobStatus::Pending);
    }

    #[test]
    fn new_job_has_retry_count_of_zero() {
        let job = Job::new(1, "Generate job".to_string());

        assert_eq!(job.retry_count, 0);
    }

    #[test]
    fn mark_running_changes_status_from_pending_to_running() {
        let mut job = Job::new(1, "Generate job".to_string());
        assert_eq!(job.status(), JobStatus::Pending);

        job.mark_running().unwrap();

        assert_eq!(job.status(), JobStatus::Running);
    }

    #[test]
    fn mark_completed_changes_status_from_running_to_completed() {
        let mut job = Job::new(1, "Generate job".to_string());
        job.mark_running().unwrap();

        job.mark_completed().unwrap();

        assert_eq!(job.status(), JobStatus::Completed);
    }

    #[test]
    fn mark_failed_changes_status_from_running_to_failed() {
        let mut job = Job::new(1, "Generate job".to_string());
        job.mark_running().unwrap();

        job.mark_failed().unwrap();

        assert_eq!(job.status(), JobStatus::Failed);
    }

    #[test]
    fn mark_completed_on_pending_job_returns_error_and_keeps_status() {
        let mut job = Job::new(1, "Generate job".to_string());

        let result = job.mark_completed();

        assert_eq!(result, Err(JobError::InvalidTransition));
        assert_eq!(job.status(), JobStatus::Pending);
    }

    #[test]
    fn mark_running_on_completed_job_returns_error_and_keeps_status() {
        let mut job = Job::new(1, "Generate job".to_string());
        job.mark_running().unwrap();
        job.mark_completed().unwrap();

        let result = job.mark_running();

        assert_eq!(result, Err(JobError::InvalidTransition));
        assert_eq!(job.status(), JobStatus::Completed);
    }

    #[test]
    fn retry_on_failed_job_sets_pending_and_increments_retry_count() {
        let mut job = Job::new(1, "Generate job".to_string());
        job.mark_running().unwrap();
        job.mark_failed().unwrap();

        job.retry().unwrap();

        assert_eq!(job.status(), JobStatus::Pending);
        assert_eq!(job.retry_count, 1);
    }

    #[test]
    fn retrying_twice_gives_retry_count_of_two() {
        let mut job = Job::new(1, "Generate job".to_string());

        job.mark_running().unwrap();
        job.mark_failed().unwrap();
        job.retry().unwrap();
        job.mark_running().unwrap();
        job.mark_failed().unwrap();
        job.retry().unwrap();

        assert_eq!(job.retry_count, 2);
    }

    #[test]
    fn retry_on_running_job_returns_error_and_keeps_state() {
        let mut job = Job::new(1, "Generate job".to_string());
        job.mark_running().unwrap();

        let result = job.retry();

        assert_eq!(result, Err(JobError::InvalidTransition));
        assert_eq!(job.status(), JobStatus::Running);
        assert_eq!(job.retry_count, 0);
    }

    #[test]
    fn retry_succeeds_up_to_the_limit() {
        let mut job = failed_job_with_retries(MAX_RETRIES - 1);

        job.retry().unwrap();

        assert_eq!(job.status(), JobStatus::Pending);
        assert_eq!(job.retry_count, MAX_RETRIES);
    }

    #[test]
    fn retry_beyond_limit_returns_error_and_keeps_state() {
        let mut job = failed_job_with_retries(MAX_RETRIES);

        let result = job.retry();

        assert_eq!(result, Err(JobError::RetryLimitReached));
        assert_eq!(job.status(), JobStatus::Failed);
        assert_eq!(job.retry_count, MAX_RETRIES);
    }

    #[test]
    fn backoff_delay_doubles_with_each_retry() {
        let mut job = Job::new(1, "Generate job".to_string());

        assert_eq!(job.backoff_delay(), Duration::from_millis(100));

        job.retry_count = 1;
        assert_eq!(job.backoff_delay(), Duration::from_millis(200));

        job.retry_count = 2;
        assert_eq!(job.backoff_delay(), Duration::from_millis(400));
    }

    #[test]
    fn backoff_delay_is_capped_at_the_maximum() {
        let mut job = Job::new(1, "Generate job".to_string());
        job.retry_count = 50;

        assert_eq!(job.backoff_delay(), MAX_BACKOFF);
    }

    #[test]
    fn can_retry_is_true_for_job_that_failed_once() {
        let job = failed_job_with_retries(0);

        assert!(job.can_retry());
    }

    #[test]
    fn can_retry_is_false_at_the_retry_limit() {
        let job = failed_job_with_retries(MAX_RETRIES);

        assert!(!job.can_retry());
    }

    #[test]
    fn can_retry_is_false_for_pending_job() {
        let job = Job::new(1, "Generate job".to_string());

        assert!(!job.can_retry());
    }
}