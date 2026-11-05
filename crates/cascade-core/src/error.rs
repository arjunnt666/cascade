use thiserror::Error;

pub type Result<T> = std::result::Result<T, CascadeError>;

#[derive(Debug, Error)]
pub enum CascadeError {
    #[error("workflow not found: {0}")]
    WorkflowNotFound(String),

    #[error("run not found: {0}")]
    RunNotFound(String),

    #[error("activity failed: {0}")]
    ActivityFailed(String),

    #[error("workflow failed: {0}")]
    WorkflowFailed(String),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("cancelled")]
    Cancelled,

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage error: {0}")]
    Storage(String),

    /// Workflow code took a different path on replay than the one recorded
    /// in history. Usually means the code changed under a running workflow.
    #[error("non-deterministic workflow: {0}")]
    NonDeterminism(String),

    #[error("history sequence mismatch: expected {expected}, got {got}")]
    SequenceMismatch { expected: u64, got: u64 },

    #[error("unknown workflow type: {0}")]
    UnknownWorkflow(String),

    #[error("unknown activity type: {0}")]
    UnknownActivity(String),

    #[error("invalid state transition: {0}")]
    InvalidTransition(String),

    #[error("internal: {0}")]
    Internal(String),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}
