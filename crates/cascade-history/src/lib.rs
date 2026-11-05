//! Event history storage.
//!
//! One append-only log per run. Appends carry the sequence number the caller
//! expects to write next, so two writers racing on the same run can't both
//! win: the loser gets `SequenceMismatch` and has to reload.
//!
//! Two backends: `MemoryHistoryStore` for tests and embeds, and
//! `FileHistoryStore`, one JSON-lines file per run.

pub mod file;
pub mod memory;
pub mod snapshot;
pub mod store;

pub use file::FileHistoryStore;
pub use memory::MemoryHistoryStore;
pub use snapshot::Snapshot;
pub use store::{AppendResult, HistoryStore};

use cascade_core::{HistoryEvent, Result, RunId};

#[async_trait::async_trait]
pub trait HistoryBackend: Send + Sync {
    /// Append `events` to `run_id`. `events[0].sequence` must equal the
    /// current length of the log, and the rest must follow on.
    async fn append(&self, run_id: &RunId, events: Vec<HistoryEvent>) -> Result<AppendResult>;
    async fn get_events(&self, run_id: &RunId, from_seq: u64) -> Result<Vec<HistoryEvent>>;
    async fn list_runs(&self) -> Result<Vec<RunId>>;
    async fn get_latest_snapshot(&self, run_id: &RunId) -> Result<Option<Snapshot>>;
    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()>;
}

/// Shared check for backends: are `events` the next ones after `len`?
pub(crate) fn check_sequence(len: u64, events: &[HistoryEvent]) -> Result<()> {
    for (i, e) in events.iter().enumerate() {
        let expected = len + i as u64;
        if e.sequence != expected {
            return Err(cascade_core::CascadeError::SequenceMismatch {
                expected,
                got: e.sequence,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod testutil {
    use cascade_core::{EventPayload, HistoryEvent, Payload, RunId, WorkflowId};

    pub fn started(run: RunId, wf: WorkflowId, seq: u64) -> HistoryEvent {
        HistoryEvent::new(
            run,
            wf,
            seq,
            EventPayload::WorkflowStarted {
                workflow_type: "demo".into(),
                input: Payload::empty(),
                task_queue: "default".into(),
            },
        )
    }

    pub fn marker(run: RunId, wf: WorkflowId, seq: u64) -> HistoryEvent {
        HistoryEvent::new(
            run,
            wf,
            seq,
            EventPayload::MarkerRecorded {
                name: "m".into(),
                value: Payload::empty(),
            },
        )
    }
}
