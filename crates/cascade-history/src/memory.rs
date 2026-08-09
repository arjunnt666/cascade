use std::collections::HashMap;
use std::sync::Arc;

use cascade_core::{HistoryEvent, RunId, Result, CascadeError};
use parking_lot::RwLock;

use crate::{AppendResult, HistoryBackend, Snapshot};

/// Pure in-memory history. Good for tests and single-process embeds.
#[derive(Default)]
pub struct MemoryHistoryStore {
    inner: Arc<RwLock<HashMap<RunId, Vec<HistoryEvent>>>>,
    snapshots: Arc<RwLock<HashMap<RunId, Snapshot>>>,
}

impl MemoryHistoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl HistoryBackend for MemoryHistoryStore {
    async fn append(&self, run_id: &RunId, events: Vec<HistoryEvent>) -> Result<AppendResult> {
        let mut map = self.inner.write();
        let entry = map.entry(*run_id).or_default();
        let first = entry.len() as u64;
        let count = events.len();
        entry.extend(events);
        let last = (entry.len() as u64).saturating_sub(1);
        Ok(AppendResult {
            first_sequence: first,
            last_sequence: last,
            events_appended: count,
        })
    }

    async fn get_events(&self, run_id: &RunId, from_seq: u64) -> Result<Vec<HistoryEvent>> {
        let map = self.inner.read();
        match map.get(run_id) {
            Some(events) => {
                let start = from_seq as usize;
                if start >= events.len() {
                    Ok(vec![])
                } else {
                    Ok(events[start..].to_vec())
                }
            }
            None => Err(CascadeError::RunNotFound(run_id.to_string())),
        }
    }

    async fn get_latest_snapshot(&self, run_id: &RunId) -> Result<Option<Snapshot>> {
        let snaps = self.snapshots.read();
        Ok(snaps.get(run_id).cloned())
    }

    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let mut snaps = self.snapshots.write();
        snaps.insert(snapshot.run_id, snapshot);
        Ok(())
    }
}
