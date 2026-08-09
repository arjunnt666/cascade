use chrono::Duration;
use serde::{Deserialize, Serialize};

use crate::id::ActivityId;
use crate::payload::Payload;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityInfo {
    pub id: ActivityId,
    pub activity_type: String,
    pub status: ActivityStatus,
    pub attempt: u32,
    pub input: Payload,
    pub result: Option<Payload>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActivityStatus {
    Scheduled,
    Started,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityOptions {
    pub start_to_close_timeout: Option<Duration>,
    pub schedule_to_close_timeout: Option<Duration>,
    pub schedule_to_start_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub retry_policy: RetryPolicy,
    pub task_queue: Option<String>,
}

impl Default for ActivityOptions {
    fn default() -> Self {
        Self {
            start_to_close_timeout: Some(Duration::seconds(30)),
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            heartbeat_timeout: None,
            retry_policy: RetryPolicy::default(),
            task_queue: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub initial_interval: Duration,
    pub backoff_coefficient: f64,
    pub maximum_interval: Duration,
    pub maximum_attempts: u32,
    pub non_retryable_errors: Vec<String>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_interval: Duration::seconds(1),
            backoff_coefficient: 2.0,
            maximum_interval: Duration::seconds(100),
            maximum_attempts: 5,
            non_retryable_errors: vec![],
        }
    }
}
