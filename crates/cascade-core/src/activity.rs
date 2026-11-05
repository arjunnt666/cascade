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

impl RetryPolicy {
    /// Never retry.
    pub fn none() -> Self {
        Self {
            maximum_attempts: 1,
            ..Self::default()
        }
    }

    /// Delay before attempt `attempt + 1`, given that `attempt` (1-based)
    /// just failed. Exponential, capped at `maximum_interval`. `jitter` in
    /// [0, 1) picks a point in the upper half of the window so retries from
    /// many runs don't line up.
    pub fn backoff(&self, attempt: u32, jitter: f64) -> std::time::Duration {
        let base = self.initial_interval.num_milliseconds().max(0) as f64;
        let cap = self.maximum_interval.num_milliseconds().max(0) as f64;
        let exp = self
            .backoff_coefficient
            .max(1.0)
            .powi(attempt.saturating_sub(1) as i32);
        let ms = (base * exp).min(cap);
        let ms = ms * (0.5 + 0.5 * jitter.clamp(0.0, 1.0));
        std::time::Duration::from_millis(ms as u64)
    }

    /// Whether another attempt is allowed after `attempt` failed with `reason`.
    /// `maximum_attempts == 0` means unlimited.
    pub fn should_retry(&self, attempt: u32, reason: &str) -> bool {
        let under_limit = self.maximum_attempts == 0 || attempt < self.maximum_attempts;
        under_limit
            && !self
                .non_retryable_errors
                .iter()
                .any(|e| reason.contains(e.as_str()))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let p = RetryPolicy {
            initial_interval: Duration::milliseconds(100),
            backoff_coefficient: 2.0,
            maximum_interval: Duration::milliseconds(1000),
            maximum_attempts: 10,
            non_retryable_errors: vec![],
        };
        let ms = |a| p.backoff(a, 1.0).as_millis();
        assert_eq!(
            [ms(1), ms(2), ms(3), ms(4), ms(5), ms(9)],
            [100, 200, 400, 800, 1000, 1000]
        );
        assert_eq!(p.backoff(3, 0.0).as_millis(), 200);
    }

    #[test]
    fn retry_limits() {
        let p = RetryPolicy {
            maximum_attempts: 3,
            non_retryable_errors: vec!["card declined".into()],
            ..RetryPolicy::default()
        };
        assert!(p.should_retry(1, "timeout"));
        assert!(p.should_retry(2, "timeout"));
        assert!(!p.should_retry(3, "timeout"));
        assert!(!p.should_retry(1, "charge failed: card declined"));
        let unlimited = RetryPolicy {
            maximum_attempts: 0,
            ..RetryPolicy::default()
        };
        assert!(unlimited.should_retry(1000, "x"));
    }
}
