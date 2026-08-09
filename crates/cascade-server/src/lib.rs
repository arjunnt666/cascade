//! Cascade server surface (gRPC + HTTP stubs).

use cascade_core::{Payload, Result, WorkflowId, RunId, WorkflowOptions};
use cascade_runtime::RuntimeEngine;
use std::sync::Arc;

pub struct Server {
    runtime: Arc<RuntimeEngine>,
}

impl Server {
    pub fn new(runtime: Arc<RuntimeEngine>) -> Self {
        Self { runtime }
    }

    pub async fn start_workflow(
        &self,
        workflow_type: &str,
        input: Payload,
        options: WorkflowOptions,
    ) -> Result<(WorkflowId, RunId)> {
        self.runtime.start(workflow_type, input, options).await
    }

    pub async fn get_status(&self, run_id: &RunId) -> Result<cascade_core::WorkflowStatus> {
        self.runtime.get_status(run_id).await
    }

    pub async fn serve(self, _addr: &str) -> Result<()> {
        tracing::info!("cascade server would listen here (stub)");
        std::future::pending::<()>().await;
        Ok(())
    }
}
