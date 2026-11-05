//! Workflow runtime.
//!
//! `Cascade` is the normal way in: register workflows and activities, start
//! runs, wait for results. `RuntimeEngine` is the layer under it, where you
//! append decisions and results by hand.

pub mod context;
pub mod decision;
pub mod engine;
pub mod host;

pub use cascade_worker::ActivityError;
pub use context::{Saga, WorkflowContext};
pub use decision::{Decision, DecisionKind};
pub use engine::RuntimeEngine;
pub use host::{Cascade, CascadeBuilder};
