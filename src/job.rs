use std::time::Duration;

const MAX_RETRIES: u32 = 3;
const BASE_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub(crate) struct Job {
    pub id: u64,
    pub payload: String,
    status: JobStatus,
    pub retry_count: u32,
}

impl Job {
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

    pub(crate) fn mark_running(&mut self) -> Result<(), JobError> {
        match self.status {
            JobStatus::Pending => {
                self.status = JobStatus::Running;
                Ok(())
            }
            _ => Err(JobError::InvalidTransition),
        }
    }

    pub(crate) fn mark_completed(&mut self) -> Result<(), JobError> {
        match self.status {
            JobStatus::Running => {
                self.status = JobStatus::Completed;
                Ok(())
            }
            _ => Err(JobError::InvalidTransition),
        }
    }

    pub(crate) fn mark_failed(&mut self) -> Result<(), JobError> {
        match self.status {
            JobStatus::Running => {
                self.status = JobStatus::Failed;
                Ok(())
            }
            _ => Err(JobError::InvalidTransition),
        }
    }

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

    /// How long to wait before the next retry: doubles with each retry, up to a maximum.
    pub(crate) fn backoff_delay(&self) -> Duration {
        let factor = 2u32.saturating_pow(self.retry_count);

        BASE_BACKOFF.saturating_mul(factor).min(MAX_BACKOFF)
    }

    pub(crate) fn can_retry(&self) -> bool {
        self.status == JobStatus::Failed && self.retry_count < MAX_RETRIES
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub(crate) enum JobStatus {
    #[default]
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, PartialEq)]
pub(crate) enum JobError {
    InvalidTransition,
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