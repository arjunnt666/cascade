//! Workflow runtime — the bit that actually drives a run forward.
//!
//! Deterministic replay from history + decision making for the next steps.

pub mod engine;
pub mod context;
pub mod decision;

pub use engine::RuntimeEngine;
pub use context::WorkflowContext;
pub use decision::{Decision, DecisionKind};

use cascade_core::{Result, RunId, WorkflowId};

/// Entry point for starting or continuing a workflow run.
#[async_trait::async_trait]
pub trait Runtime: Send + Sync {
    async fn start_workflow(
        &self,
        workflow_type: &str,
        input: cascade_core::Payload,
        options: cascade_core::WorkflowOptions,
    ) -> Result<(WorkflowId, RunId)>;

    async fn signal_workflow(
        &self,
        workflow_id: &WorkflowId,
        signal_name: &str,
        input: cascade_core::Payload,
    ) -> Result<()>;

    async fn cancel_workflow(&self, workflow_id: &WorkflowId) -> Result<()>;
}
