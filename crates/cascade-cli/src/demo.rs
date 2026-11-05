//! Workflows `cascade serve` registers so there is something to poke at.

use std::time::Duration;

use cascade_core::Result;
use cascade_runtime::{ActivityError, CascadeBuilder, WorkflowContext};
use serde_json::{json, Value};

pub fn register(b: CascadeBuilder) -> CascadeBuilder {
    b.workflow("order", order)
        .workflow("approval", approval)
        .workflow("sleep", sleep)
        .activity("price", |sku: String| async move {
            if sku.is_empty() {
                return Err(ActivityError::fatal("empty sku"));
            }
            Ok(100 * sku.len() as u64)
        })
        .activity("charge", |cents: u64| async move {
            // fails now and then so the retries show up in history
            if fastrand::u8(..3) == 0 {
                return Err(ActivityError::retryable("payment gateway timeout"));
            }
            Ok(format!("ch_{cents}"))
        })
}

/// {"sku": "..."} -> price it, charge it.
async fn order(ctx: WorkflowContext, input: Value) -> Result<Value> {
    let sku = input["sku"].as_str().unwrap_or_default().to_string();
    let cents: u64 = ctx.activity("price", &sku).await?;
    let charge: String = ctx.activity("charge", &cents).await?;
    Ok(json!({ "sku": sku, "cents": cents, "charge": charge }))
}

/// Waits up to `timeout_secs` for an "approve" signal.
async fn approval(ctx: WorkflowContext, input: Value) -> Result<Value> {
    let secs = input["timeout_secs"].as_u64().unwrap_or(300);
    tokio::select! {
        biased;
        by = ctx.signal::<Value>("approve") => Ok(json!({ "approved": true, "by": by? })),
        t = ctx.sleep(Duration::from_secs(secs)) => { t?; Ok(json!({ "approved": false })) }
    }
}

async fn sleep(ctx: WorkflowContext, secs: u64) -> Result<Value> {
    let started = ctx.now();
    ctx.sleep(Duration::from_secs(secs)).await?;
    Ok(json!({ "slept_from": started, "woke_at": ctx.now() }))
}
