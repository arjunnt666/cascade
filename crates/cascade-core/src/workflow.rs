use serde::{Deserialize, Serialize};

use crate::id::{RunId, WorkflowId};
use crate::payload::Payload;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowInfo {
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub workflow_type: String,
    pub status: WorkflowStatus,
    pub task_queue: String,
    pub input: Payload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    ContinuedAsNew,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowOptions {
    pub task_queue: String,
    pub workflow_execution_timeout: Option<chrono::Duration>,
    pub workflow_run_timeout: Option<chrono::Duration>,
    pub workflow_task_timeout: Option<chrono::Duration>,
    pub retry_policy: Option<crate::activity::RetryPolicy>,
    pub cron_schedule: Option<String>,
}

impl Default for WorkflowOptions {
    fn default() -> Self {
        Self {
            task_queue: "default".into(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Some(chrono::Duration::seconds(10)),
            retry_policy: None,
            cron_schedule: None,
        }
    }
}
