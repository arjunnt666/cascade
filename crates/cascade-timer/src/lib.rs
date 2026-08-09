//! Durable timer service.

use chrono::{DateTime, Utc};
use cascade_core::{Result, TimerId, RunId};

pub mod wheel;
pub use wheel::TimerWheel;

#[derive(Debug, Clone)]
pub struct TimerEntry {
    pub id: TimerId,
    pub run_id: RunId,
    pub fire_at: DateTime<Utc>,
}

#[async_trait::async_trait]
pub trait TimerService: Send + Sync {
    async fn schedule(&self, entry: TimerEntry) -> Result<()>;
    async fn cancel(&self, id: &TimerId) -> Result<()>;
    async fn poll_due(&self, now: DateTime<Utc>) -> Result<Vec<TimerEntry>>;
}
