use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::id::{ActivityId, EventId, RunId, TimerId, WorkflowId};
use crate::payload::Payload;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEvent {
    pub id: EventId,
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    pub sequence: u64,
    pub event_type: EventType,
    pub payload: EventPayload,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    WorkflowStarted,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowCancelled,
    ActivityScheduled,
    ActivityStarted,
    ActivityCompleted,
    ActivityFailed,
    ActivityTimedOut,
    TimerStarted,
    TimerFired,
    TimerCancelled,
    MarkerRecorded,
    SignalReceived,
    UpsertSearchAttributes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventPayload {
    WorkflowStarted {
        workflow_type: String,
        input: Payload,
        task_queue: String,
    },
    WorkflowCompleted {
        result: Payload,
    },
    WorkflowFailed {
        reason: String,
    },
    ActivityScheduled {
        activity_id: ActivityId,
        activity_type: String,
        input: Payload,
        options: crate::activity::ActivityOptions,
    },
    ActivityCompleted {
        activity_id: ActivityId,
        result: Payload,
    },
    ActivityFailed {
        activity_id: ActivityId,
        reason: String,
        attempt: u32,
    },
    TimerStarted {
        timer_id: TimerId,
        fire_at: DateTime<Utc>,
    },
    TimerFired {
        timer_id: TimerId,
    },
    SignalReceived {
        signal_name: String,
        input: Payload,
    },
    Empty,
}
