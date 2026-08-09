//! Activity worker pool.

use cascade_core::{ActivityId, Payload, Result, RunId};

pub mod pool;
pub use pool::WorkerPool;

#[async_trait::async_trait]
pub trait ActivityExecutor: Send + Sync {
    async fn execute(
        &self,
        activity_type: &str,
        input: Payload,
    ) -> Result<Payload>;
}

#[derive(Debug, Clone)]
pub struct ActivityTask {
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub activity_type: String,
    pub input: Payload,
    pub attempt: u32,
}

#[derive(Debug, Clone)]
pub enum ActivityResult {
    Completed { activity_id: ActivityId, result: Payload },
    Failed { activity_id: ActivityId, reason: String, attempt: u32 },
}
