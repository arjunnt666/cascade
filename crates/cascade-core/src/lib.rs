//! Types shared by every cascade crate: ids, history events, payloads,
//! options and the error type.

pub mod activity;
pub mod error;
pub mod event;
pub mod id;
pub mod payload;
pub mod workflow;

pub use activity::{ActivityInfo, ActivityOptions, ActivityStatus, RetryPolicy};
pub use error::{CascadeError, Result};
pub use event::{EventPayload, EventType, HistoryEvent};
pub use id::{ActivityId, EventId, RunId, TimerId, WorkflowId};
pub use payload::Payload;
pub use workflow::{WorkflowInfo, WorkflowOptions, WorkflowStatus};

/// Namespace for logical isolation of workflow types / task queues.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Namespace(pub String);

impl Namespace {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl Default for Namespace {
    fn default() -> Self {
        Self("default".into())
    }
}
