//! Durable event history store for Cascade.
//!
//! Append-only log per run + optional snapshots for fast recovery.
//! In-memory and (stub) durable backends.

pub mod store;
pub mod memory;
pub mod snapshot;

pub use store::{HistoryStore, AppendResult};
pub use memory::MemoryHistoryStore;
pub use snapshot::Snapshot;

use cascade_core::{HistoryEvent, RunId, Result};

/// Trait for any history backend.
#[async_trait::async_trait]
pub trait HistoryBackend: Send + Sync {
    async fn append(&self, run_id: &RunId, events: Vec<HistoryEvent>) -> Result<AppendResult>;
    async fn get_events(&self, run_id: &RunId, from_seq: u64) -> Result<Vec<HistoryEvent>>;
    async fn get_latest_snapshot(&self, run_id: &RunId) -> Result<Option<Snapshot>>;
    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()>;
}
