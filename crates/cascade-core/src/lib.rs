//! Core types for Cascade durable workflows.
//!
//! Events, identifiers, errors, and the basic vocabulary everything else speaks.

pub mod error;
pub mod id;
pub mod event;
pub mod activity;
pub mod workflow;
pub mod payload;

pub use error::{CascadeError, Result};
pub use id::{WorkflowId, RunId, ActivityId, TimerId, EventId};
pub use event::{HistoryEvent, EventType, EventPayload};
pub use activity::{ActivityInfo, ActivityStatus, ActivityOptions};
pub use workflow::{WorkflowInfo, WorkflowStatus, WorkflowOptions};
pub use payload::Payload;

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
