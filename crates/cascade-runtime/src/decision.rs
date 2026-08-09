use cascade_core::{ActivityId, ActivityOptions, Payload, TimerId};
use chrono::{DateTime, Utc};

/// Decisions the workflow code makes. These become history events + tasks.
#[derive(Debug, Clone)]
pub struct Decision {
    pub kind: DecisionKind,
}

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
    ContinueAsNew {
        input: Payload,
    },
}
