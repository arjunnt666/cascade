//! Activity registry and a small worker pool.
//!
//! Activities are plain async functions from a JSON-decodable input to a
//! JSON-encodable output. They run outside workflow replay, so they can do
//! anything: IO, clocks, randomness.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use cascade_core::{ActivityId, Payload, RunId};
use serde::{de::DeserializeOwned, Serialize};

pub mod pool;
pub use pool::WorkerPool;

/// Error returned from an activity. Retryable unless built with `fatal`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityError {
    pub message: String,
    pub non_retryable: bool,
}

impl ActivityError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: false,
        }
    }

    /// Don't bother retrying, it will fail the same way.
    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: true,
        }
    }
}

impl fmt::Display for ActivityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

// Lets `?` work on io errors, serde errors and friends inside an activity.
impl<E: std::error::Error> From<E> for ActivityError {
    fn from(e: E) -> Self {
        Self::retryable(e.to_string())
    }
}

pub type ActivityResultFuture =
    Pin<Box<dyn Future<Output = Result<Payload, ActivityError>> + Send>>;
pub type ActivityFn = Arc<dyn Fn(Payload) -> ActivityResultFuture + Send + Sync>;

#[async_trait::async_trait]
pub trait ActivityExecutor: Send + Sync {
    async fn execute(&self, activity_type: &str, input: Payload) -> Result<Payload, ActivityError>;
}

/// Activities by name.
#[derive(Clone, Default)]
pub struct ActivityRegistry {
    activities: HashMap<String, ActivityFn>,
}

impl ActivityRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a typed activity. Input and output go through JSON.
    pub fn register<I, O, F, Fut>(&mut self, name: impl Into<String>, f: F)
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, ActivityError>> + Send + 'static,
    {
        let f = Arc::new(f);
        let wrapped: ActivityFn = Arc::new(move |payload: Payload| {
            let f = f.clone();
            Box::pin(async move {
                let input: I = payload
                    .as_json()
                    .map_err(|e| ActivityError::fatal(format!("bad activity input: {e}")))?;
                let out = f(input).await?;
                Payload::from_json(&out)
                    .map_err(|e| ActivityError::fatal(format!("bad activity output: {e}")))
            })
        });
        self.activities.insert(name.into(), wrapped);
    }

    pub fn contains(&self, name: &str) -> bool {
        self.activities.contains_key(name)
    }

    pub fn names(&self) -> Vec<&str> {
        let mut v: Vec<_> = self.activities.keys().map(String::as_str).collect();
        v.sort_unstable();
        v
    }
}

#[async_trait::async_trait]
impl ActivityExecutor for ActivityRegistry {
    async fn execute(&self, activity_type: &str, input: Payload) -> Result<Payload, ActivityError> {
        match self.activities.get(activity_type) {
            Some(f) => f(input).await,
            None => Err(ActivityError::fatal(format!(
                "no activity registered as {activity_type:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActivityTask {
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub activity_type: String,
    pub input: Payload,
    pub attempt: u32,
}

#[derive(Debug, Clone)]
pub enum ActivityResult {
    Completed {
        activity_id: ActivityId,
        result: Payload,
    },
    Failed {
        activity_id: ActivityId,
        error: ActivityError,
        attempt: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Add {
        a: i64,
        b: i64,
    }

    #[tokio::test]
    async fn typed_registration_roundtrips_json() {
        let mut reg = ActivityRegistry::new();
        reg.register("add", |x: Add| async move { Ok(x.a + x.b) });
        let out = reg
            .execute(
                "add",
                Payload::from_json(&serde_json::json!({"a": 2, "b": 3})).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(out.as_json::<i64>().unwrap(), 5);
    }

    #[tokio::test]
    async fn bad_input_and_unknown_names_are_fatal() {
        let mut reg = ActivityRegistry::new();
        reg.register("add", |x: Add| async move { Ok(x.a + x.b) });
        let err = reg
            .execute("add", Payload::from_json(&"nope").unwrap())
            .await
            .unwrap_err();
        assert!(err.non_retryable);
        let err = reg.execute("missing", Payload::empty()).await.unwrap_err();
        assert!(err.non_retryable);
    }

    #[tokio::test]
    async fn question_mark_on_std_errors() {
        let mut reg = ActivityRegistry::new();
        reg.register("parse", |s: String| async move { Ok(s.parse::<u32>()?) });
        let err = reg
            .execute("parse", Payload::from_json(&"x").unwrap())
            .await
            .unwrap_err();
        assert!(!err.non_retryable);
        assert!(err.message.contains("invalid digit"));
    }
}
