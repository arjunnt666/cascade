use cascade_core::{ActivityId, ActivityOptions, Payload, TimerId};
use chrono::{DateTime, Utc};

/// Something workflow code asked for. Applying a decision appends the
/// matching event to history.
#[derive(Debug, Clone)]
pub struct Decision {
    pub kind: DecisionKind,
}

impl From<DecisionKind> for Decision {
    fn from(kind: DecisionKind) -> Self {
        Self { kind }
    }
}

// Lives for one drive, never stored in bulk; boxing would just add noise.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum DecisionKind {
    ScheduleActivity {
        activity_id: ActivityId,
        activity_type: String,
        input: Payload,
        options: ActivityOptions,
    },
    CompleteWorkflow {
        result: Payload,
    },
    FailWorkflow {
        reason: String,
    },
    StartTimer {
        timer_id: TimerId,
        fire_at: DateTime<Utc>,
    },
    CancelTimer {
        timer_id: TimerId,
    },
    RecordMarker {
        name: String,
        value: Payload,
    },
    ContinueAsNew {
        input: Payload,
    },
}

impl DecisionKind {
    pub fn into_payload(self) -> cascade_core::EventPayload {
        use cascade_core::EventPayload as P;
        match self {
            DecisionKind::ScheduleActivity {
                activity_id,
                activity_type,
                input,
                options,
            } => P::ActivityScheduled {
                activity_id,
                activity_type,
                input,
                options,
            },
            DecisionKind::CompleteWorkflow { result } => P::WorkflowCompleted { result },
            DecisionKind::FailWorkflow { reason } => P::WorkflowFailed { reason },
            DecisionKind::StartTimer { timer_id, fire_at } => P::TimerStarted { timer_id, fire_at },
            DecisionKind::CancelTimer { timer_id } => P::TimerCancelled { timer_id },
            DecisionKind::RecordMarker { name, value } => P::MarkerRecorded { name, value },
            DecisionKind::ContinueAsNew { input } => P::WorkflowContinuedAsNew { input },
        }
    }
}
