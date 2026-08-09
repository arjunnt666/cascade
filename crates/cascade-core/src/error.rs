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

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("cancelled")]
    Cancelled,

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("invalid state transition: {0}")]
    InvalidTransition(String),

    #[error("internal: {0}")]
    Internal(String),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}
