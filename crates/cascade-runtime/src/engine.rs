use std::sync::Arc;

use cascade_core::{
    HistoryEvent, Payload, Result, RunId, WorkflowId, WorkflowOptions, WorkflowStatus,
};
use cascade_history::{HistoryStore, MemoryHistoryStore};
use tracing::{debug, info};

use crate::decision::Decision;

/// Simple single-process runtime engine. History is the source of truth.
pub struct RuntimeEngine {
    history: Arc<HistoryStore<MemoryHistoryStore>>,
}

impl RuntimeEngine {
    pub fn new() -> Self {
        Self {
            history: Arc::new(HistoryStore::new(MemoryHistoryStore::new())),
        }
    }

    pub fn with_history(history: HistoryStore<MemoryHistoryStore>) -> Self {
        Self {
            history: Arc::new(history),
        }
    }

    pub async fn start(
        &self,
        workflow_type: &str,
        input: Payload,
        options: WorkflowOptions,
    ) -> Result<(WorkflowId, RunId)> {
        let workflow_id = WorkflowId::new();
        let run_id = RunId::new();

        let start_event = HistoryEvent {
            id: cascade_core::EventId::new(),
            run_id,
            workflow_id,
            sequence: 0,
            event_type: cascade_core::EventType::WorkflowStarted,
            payload: cascade_core::EventPayload::WorkflowStarted {
                workflow_type: workflow_type.to_string(),
                input,
                task_queue: options.task_queue.clone(),
            },
            timestamp: chrono::Utc::now(),
        };

        self.history.append(&run_id, vec![start_event]).await?;
        info!(%workflow_id, %run_id, workflow_type, "workflow started");
        Ok((workflow_id, run_id))
    }

    pub async fn apply_decisions(&self, run_id: &RunId, decisions: Vec<Decision>) -> Result<()> {
        debug!(%run_id, count = decisions.len(), "applying decisions (stub)");
        Ok(())
    }

    pub async fn get_status(&self, run_id: &RunId) -> Result<WorkflowStatus> {
        let events = self.history.load(run_id, 0).await?;
        if let Some(last) = events.last() {
            match last.event_type {
                cascade_core::EventType::WorkflowCompleted => Ok(WorkflowStatus::Completed),
                cascade_core::EventType::WorkflowFailed => Ok(WorkflowStatus::Failed),
                cascade_core::EventType::WorkflowCancelled => Ok(WorkflowStatus::Cancelled),
                _ => Ok(WorkflowStatus::Running),
            }
        } else {
            Ok(WorkflowStatus::Running)
        }
    }
}

impl Default for RuntimeEngine {
    fn default() -> Self {
        Self::new()
    }
}
