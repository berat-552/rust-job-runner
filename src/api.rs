use crate::job::{Job, JobStatus};
use crate::queue::{JobQueue, QueueError};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) queue: Arc<Mutex<JobQueue>>,
    next_id: Arc<AtomicU64>,
}

impl AppState {
    pub(crate) fn new(queue: Arc<Mutex<JobQueue>>) -> Self {
        Self {
            queue,
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct SubmitRequest {
    pub payload: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct SubmitResponse {
    pub id: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct StatusResponse {
    pub id: u64,
    pub status: String,
}

pub(crate) fn router(state: AppState) -> Router {
    Router::new()
        .route("/jobs", post(submit_job))
        .route("/jobs/{id}", get(get_job))
        .with_state(state)
}

async fn submit_job(
    State(state): State<AppState>,
    Json(request): Json<SubmitRequest>,
) -> Result<(StatusCode, Json<SubmitResponse>), StatusCode> {
    let id = state.next_id.fetch_add(1, Ordering::SeqCst);

    let result = state
        .queue
        .lock()
        .await
        .submit(Job::new(id, request.payload));

    match result {
        Ok(()) => Ok((StatusCode::CREATED, Json(SubmitResponse { id }))),
        Err(QueueError::QueueFull) => Err(StatusCode::SERVICE_UNAVAILABLE),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn status_name(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Pending => "pending",
        JobStatus::Running => "running",
        JobStatus::Completed => "completed",
        JobStatus::Failed => "failed",
    }
}

async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<StatusResponse>, StatusCode> {
    match state.queue.lock().await.status_of(id) {
        Some(status) => Ok(Json(StatusResponse {
            id,
            status: status_name(status).to_string(),
        })),
        None => Err(StatusCode::NOT_FOUND),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_name_maps_every_status() {
        assert_eq!(status_name(JobStatus::Pending), "pending");
        assert_eq!(status_name(JobStatus::Running), "running");
        assert_eq!(status_name(JobStatus::Completed), "completed");
        assert_eq!(status_name(JobStatus::Failed), "failed");
    }

    #[tokio::test]
    async fn get_submitted_job_returns_pending() {
        let state = test_state(10);

        let _ = submit_job(State(state.clone()), request("work"))
            .await
            .unwrap();

        let result = get_job(State(state), Path(1)).await;

        let Json(body) = result.unwrap();
        assert_eq!(body.id, 1);
        assert_eq!(body.status, "pending");
    }

    #[tokio::test]
    async fn get_unknown_job_returns_not_found() {
        let state = test_state(10);

        let result = get_job(State(state), Path(99)).await;

        assert_eq!(result.unwrap_err(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_job_taken_by_a_worker_returns_running() {
        let state = test_state(10);
        let _ = submit_job(State(state.clone()), request("work"))
            .await
            .unwrap();

        // Do what a worker does: take the job off the queue.
        state.queue.lock().await.next_job();

        let result = get_job(State(state), Path(1)).await;

        let Json(body) = result.unwrap();
        assert_eq!(body.status, "running");
    }

    fn test_state(capacity: usize) -> AppState {
        AppState::new(Arc::new(Mutex::new(JobQueue::with_capacity(capacity))))
    }

    fn request(payload: &str) -> Json<SubmitRequest> {
        Json(SubmitRequest {
            payload: payload.to_string(),
        })
    }

    #[tokio::test]
    async fn submit_returns_created_with_id_one() {
        let state = test_state(10);

        let result = submit_job(State(state), request("work")).await;

        let (status, Json(body)) = result.unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body.id, 1);
    }

    #[tokio::test]
    async fn two_submits_get_ids_one_and_two() {
        let state = test_state(10);

        let first = submit_job(State(state.clone()), request("a")).await;
        let second = submit_job(State(state), request("b")).await;

        let (_, Json(first_body)) = first.unwrap();
        let (_, Json(second_body)) = second.unwrap();
        assert_eq!(first_body.id, 1);
        assert_eq!(second_body.id, 2);
    }

    #[tokio::test]
    async fn submitted_job_is_pending_in_the_queue() {
        let state = test_state(10);
       let _ = submit_job(State(state.clone()), request("work"))
            .await
            .unwrap();

        assert_eq!(
            state.queue.lock().await.status_of(1),
            Some(JobStatus::Pending)
        );
    }

    #[tokio::test]
    async fn full_queue_returns_service_unavailable_and_keeps_length() {
        let state = test_state(1);
        let _ = submit_job(State(state.clone()), request("a"))
            .await
            .unwrap();
        let result = submit_job(State(state.clone()), request("b")).await;

        assert_eq!(result.unwrap_err(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(state.queue.lock().await.len(), 1);
    }
}