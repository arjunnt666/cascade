use cascade_core::{ActivityOptions, Payload, Result};

/// Context passed into a workflow function.
/// All side-effects go through this so they can be recorded in history.
pub struct WorkflowContext {
    _private: (),
}

impl WorkflowContext {
    pub fn new() -> Self {
        Self { _private: () }
    }

    pub async fn activity<T>(
        &self,
        _name: &str,
        _input: Payload,
        _opts: ActivityOptions,
    ) -> Result<T>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        Err(cascade_core::CascadeError::Internal(
            "activity execution not implemented in this stub".into(),
        ))
    }

    pub async fn sleep(&self, _duration: chrono::Duration) -> Result<()> {
        Err(cascade_core::CascadeError::Internal(
            "timer sleep not implemented in this stub".into(),
        ))
    }

    pub async fn wait_signal<T>(&self, _name: &str) -> Result<T>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        Err(cascade_core::CascadeError::Internal(
            "signal wait not implemented in this stub".into(),
        ))
    }
}

impl Default for WorkflowContext {
    fn default() -> Self {
        Self::new()
    }
}
