use std::sync::Arc;

use cascade_core::{HistoryEvent, Result, RunId};

use crate::HistoryBackend;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResult {
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub events_appended: usize,
}

impl AppendResult {
    pub fn new(first: u64, count: usize) -> Self {
        Self {
            first_sequence: first,
            last_sequence: (first + count as u64).saturating_sub(1),
            events_appended: count,
        }
    }
}

/// Cheap-to-clone handle over any backend.
#[derive(Clone)]
pub struct HistoryStore {
    backend: Arc<dyn HistoryBackend>,
}

impl HistoryStore {
    pub fn new(backend: impl HistoryBackend + 'static) -> Self {
        Self {
            backend: Arc::new(backend),
        }
    }

    pub fn from_arc(backend: Arc<dyn HistoryBackend>) -> Self {
        Self { backend }
    }

    pub fn memory() -> Self {
        Self::new(crate::MemoryHistoryStore::new())
    }

    pub async fn append(&self, run_id: &RunId, events: Vec<HistoryEvent>) -> Result<AppendResult> {
        if events.is_empty() {
            return Ok(AppendResult::new(0, 0));
        }
        self.backend.append(run_id, events).await
    }

    pub async fn load(&self, run_id: &RunId, from_seq: u64) -> Result<Vec<HistoryEvent>> {
        self.backend.get_events(run_id, from_seq).await
    }

    pub async fn list_runs(&self) -> Result<Vec<RunId>> {
        self.backend.list_runs().await
    }

    pub fn backend(&self) -> &Arc<dyn HistoryBackend> {
        &self.backend
    }
}
