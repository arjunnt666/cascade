use std::collections::HashMap;

use cascade_core::{CascadeError, HistoryEvent, Result, RunId};
use parking_lot::RwLock;

use crate::{check_sequence, AppendResult, HistoryBackend, Snapshot};

/// History kept in a HashMap. Gone when the process exits.
#[derive(Default)]
pub struct MemoryHistoryStore {
    runs: RwLock<HashMap<RunId, Vec<HistoryEvent>>>,
    snapshots: RwLock<HashMap<RunId, Snapshot>>,
}

impl MemoryHistoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl HistoryBackend for MemoryHistoryStore {
    async fn append(&self, run_id: &RunId, events: Vec<HistoryEvent>) -> Result<AppendResult> {
        let mut map = self.runs.write();
        let log = map.entry(*run_id).or_default();
        let first = log.len() as u64;
        check_sequence(first, &events)?;
        let count = events.len();
        log.extend(events);
        Ok(AppendResult::new(first, count))
    }

    async fn get_events(&self, run_id: &RunId, from_seq: u64) -> Result<Vec<HistoryEvent>> {
        let map = self.runs.read();
        let log = map
            .get(run_id)
            .ok_or_else(|| CascadeError::RunNotFound(run_id.to_string()))?;
        Ok(log.iter().skip(from_seq as usize).cloned().collect())
    }

    async fn list_runs(&self) -> Result<Vec<RunId>> {
        let mut runs: Vec<_> = self.runs.read().keys().copied().collect();
        runs.sort();
        Ok(runs)
    }

    async fn get_latest_snapshot(&self, run_id: &RunId) -> Result<Option<Snapshot>> {
        Ok(self.snapshots.read().get(run_id).cloned())
    }

    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        self.snapshots.write().insert(snapshot.run_id, snapshot);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{marker, started};
    use cascade_core::{EventType, WorkflowId};

    #[tokio::test]
    async fn append_and_read() {
        let store = MemoryHistoryStore::new();
        let (run, wf) = (RunId::new(), WorkflowId::new());
        store.append(&run, vec![started(run, wf, 0)]).await.unwrap();
        let events = store.get_events(&run, 0).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::WorkflowStarted);
    }

    #[tokio::test]
    async fn missing_run_errors() {
        let store = MemoryHistoryStore::new();
        let err = store.get_events(&RunId::new(), 0).await.unwrap_err();
        assert!(matches!(err, CascadeError::RunNotFound(_)));
    }

    #[tokio::test]
    async fn stale_writer_is_rejected() {
        let store = MemoryHistoryStore::new();
        let (run, wf) = (RunId::new(), WorkflowId::new());
        store.append(&run, vec![started(run, wf, 0)]).await.unwrap();
        store.append(&run, vec![marker(run, wf, 1)]).await.unwrap();
        // A second writer that also thinks it is appending seq 1.
        let err = store
            .append(&run, vec![marker(run, wf, 1)])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            CascadeError::SequenceMismatch {
                expected: 2,
                got: 1
            }
        ));
    }
}
