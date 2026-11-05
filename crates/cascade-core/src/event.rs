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

impl HistoryEvent {
    pub fn new(
        run_id: RunId,
        workflow_id: WorkflowId,
        sequence: u64,
        payload: EventPayload,
    ) -> Self {
        Self {
            id: EventId::new(),
            run_id,
            workflow_id,
            sequence,
            event_type: payload.event_type(),
            payload,
            timestamp: Utc::now(),
        }
    }

    /// True for events that end a run.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.event_type,
            EventType::WorkflowCompleted
                | EventType::WorkflowFailed
                | EventType::WorkflowCancelled
                | EventType::WorkflowContinuedAsNew
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    WorkflowStarted,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowCancelled,
    WorkflowContinuedAsNew,
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
    WorkflowCancelled {
        reason: String,
    },
    WorkflowContinuedAsNew {
        input: Payload,
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
    ActivityStarted {
        activity_id: ActivityId,
        attempt: u32,
    },
    ActivityFailed {
        activity_id: ActivityId,
        reason: String,
        attempt: u32,
        /// False on the last attempt. Only a final failure is visible to
        /// workflow code; earlier ones are just a record of what happened.
        #[serde(default)]
        will_retry: bool,
    },
    TimerStarted {
        timer_id: TimerId,
        fire_at: DateTime<Utc>,
    },
    TimerFired {
        timer_id: TimerId,
    },
    TimerCancelled {
        timer_id: TimerId,
    },
    MarkerRecorded {
        name: String,
        value: Payload,
    },
    SignalReceived {
        signal_name: String,
        input: Payload,
    },
    Empty,
}

impl EventPayload {
    pub fn event_type(&self) -> EventType {
        use EventPayload as P;
        match self {
            P::WorkflowStarted { .. } => EventType::WorkflowStarted,
            P::WorkflowCompleted { .. } => EventType::WorkflowCompleted,
            P::WorkflowFailed { .. } => EventType::WorkflowFailed,
            P::WorkflowCancelled { .. } => EventType::WorkflowCancelled,
            P::WorkflowContinuedAsNew { .. } => EventType::WorkflowContinuedAsNew,
            P::ActivityScheduled { .. } => EventType::ActivityScheduled,
            P::ActivityStarted { .. } => EventType::ActivityStarted,
            P::ActivityCompleted { .. } => EventType::ActivityCompleted,
            P::ActivityFailed { .. } => EventType::ActivityFailed,
            P::TimerStarted { .. } => EventType::TimerStarted,
            P::TimerFired { .. } => EventType::TimerFired,
            P::TimerCancelled { .. } => EventType::TimerCancelled,
            P::MarkerRecorded { .. } => EventType::MarkerRecorded,
            P::SignalReceived { .. } => EventType::SignalReceived,
            P::Empty => EventType::UpsertSearchAttributes,
        }
    }
}
