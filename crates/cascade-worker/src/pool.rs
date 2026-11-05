use std::sync::Arc;

use tokio::sync::Semaphore;
use tracing::debug;

use crate::{ActivityExecutor, ActivityResult, ActivityTask};

/// Runs activity tasks with a cap on how many execute at once.
pub struct WorkerPool {
    permits: Arc<Semaphore>,
    concurrency: usize,
    executor: Arc<dyn ActivityExecutor>,
}

impl WorkerPool {
    pub fn new(concurrency: usize, executor: Arc<dyn ActivityExecutor>) -> Self {
        let concurrency = concurrency.max(1);
        Self {
            permits: Arc::new(Semaphore::new(concurrency)),
            concurrency,
            executor,
        }
    }

    pub async fn process_one(&self, task: ActivityTask) -> ActivityResult {
        let _permit = self
            .permits
            .acquire()
            .await
            .expect("semaphore never closed");
        debug!(activity_type = %task.activity_type, attempt = task.attempt, "executing activity");
        match self.executor.execute(&task.activity_type, task.input).await {
            Ok(result) => ActivityResult::Completed {
                activity_id: task.activity_id,
                result,
            },
            Err(error) => ActivityResult::Failed {
                activity_id: task.activity_id,
                error,
                attempt: task.attempt,
            },
        }
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub fn executor(&self) -> &Arc<dyn ActivityExecutor> {
        &self.executor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ActivityRegistry;
    use cascade_core::{ActivityId, Payload, RunId};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn never_runs_more_than_concurrency_at_once() {
        static RUNNING: AtomicUsize = AtomicUsize::new(0);
        static PEAK: AtomicUsize = AtomicUsize::new(0);
        let mut reg = ActivityRegistry::new();
        reg.register("slow", |_: ()| async {
            let now = RUNNING.fetch_add(1, Ordering::SeqCst) + 1;
            PEAK.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            RUNNING.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        });
        let pool = Arc::new(WorkerPool::new(2, Arc::new(reg)));
        let mut handles = Vec::new();
        for _ in 0..6 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                pool.process_one(ActivityTask {
                    activity_id: ActivityId::new(),
                    run_id: RunId::new(),
                    activity_type: "slow".into(),
                    input: Payload::empty(),
                    attempt: 1,
                })
                .await
            }));
        }
        for h in handles {
            assert!(matches!(h.await.unwrap(), ActivityResult::Completed { .. }));
        }
        assert_eq!(PEAK.load(Ordering::SeqCst), 2);
    }
}
