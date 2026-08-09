use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use cascade_core::{Payload, RunId, WorkflowId};

/// Compact state of a workflow run for faster recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    pub last_event_sequence: u64,
    pub status: cascade_core::WorkflowStatus,
    pub state_blob: Payload,
    pub taken_at: DateTime<Utc>,
}

impl Snapshot {
    pub fn new(
        run_id: RunId,
        workflow_id: WorkflowId,
        last_seq: u64,
        status: cascade_core::WorkflowStatus,
        state: Payload,
    ) -> Self {
        Self {
            run_id,
            workflow_id,
            last_event_sequence: last_seq,
            status,
            state_blob: state,
            taken_at: Utc::now(),
        }
    }
}
