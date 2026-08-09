use cascade_core::{HistoryEvent, RunId, Result};

#[derive(Debug, Clone)]
pub struct AppendResult {
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub events_appended: usize,
}

/// High-level history store that sits on top of a backend.
pub struct HistoryStore<B: crate::HistoryBackend> {
    backend: B,
}

impl<B: crate::HistoryBackend> HistoryStore<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub async fn append(&self, run_id: &RunId, events: Vec<HistoryEvent>) -> Result<AppendResult> {
        if events.is_empty() {
            return Ok(AppendResult {
                first_sequence: 0,
                last_sequence: 0,
                events_appended: 0,
            });
        }
        self.backend.append(run_id, events).await
    }

    pub async fn load(&self, run_id: &RunId, from_seq: u64) -> Result<Vec<HistoryEvent>> {
        self.backend.get_events(run_id, from_seq).await
    }
}
