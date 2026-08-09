use std::sync::Arc;

use cascade_core::Result;
use tracing::{debug, info};

use crate::{ActivityExecutor, ActivityTask, ActivityResult};

pub struct WorkerPool {
    concurrency: usize,
    executor: Arc<dyn ActivityExecutor>,
}

impl WorkerPool {
    pub fn new(concurrency: usize, executor: Arc<dyn ActivityExecutor>) -> Self {
        Self {
            concurrency,
            executor,
        }
    }

    pub async fn process_one(&self, task: ActivityTask) -> Result<ActivityResult> {
        debug!(activity_type = %task.activity_type, attempt = task.attempt, "executing activity");
        match self.executor.execute(&task.activity_type, task.input).await {
            Ok(result) => {
                info!(activity_id = %task.activity_id, "activity completed");
                Ok(ActivityResult::Completed {
                    activity_id: task.activity_id,
                    result,
                })
            }
            Err(e) => {
                info!(activity_id = %task.activity_id, error = %e, "activity failed");
                Ok(ActivityResult::Failed {
                    activity_id: task.activity_id,
                    reason: e.to_string(),
                    attempt: task.attempt,
                })
            }
        }
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }
}
