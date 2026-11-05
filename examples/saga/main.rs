//! Book a trip; if the car fails, undo the hotel and flight.
//! cargo run -p cascade-runtime --example saga

use cascade_core::{ActivityOptions, RetryPolicy};
use cascade_runtime::{ActivityError, Cascade, Saga, WorkflowContext};

#[tokio::main]
async fn main() -> cascade_core::Result<()> {
    let no_retry = ActivityOptions {
        retry_policy: RetryPolicy::none(),
        ..ActivityOptions::default()
    };
    let host = Cascade::builder()
        .workflow("trip", move |ctx: WorkflowContext, (): ()| {
            let no_retry = no_retry.clone();
            async move {
                let mut saga = Saga::new(&ctx);
                for step in ["flight", "hotel"] {
                    ctx.activity::<_, ()>("book", &step).await?;
                    saga.add("cancel", &step)?;
                }
                if let Err(e) = ctx.activity_with::<_, ()>("book", &"car", no_retry).await {
                    saga.compensate().await;
                    return Err(e);
                }
                Ok(())
            }
        })
        .activity("book", |what: String| async move {
            if what == "car" {
                return Err(ActivityError::fatal("no cars left"));
            }
            println!("booked {what}");
            Ok(())
        })
        .activity("cancel", |what: String| async move {
            println!("cancelled {what}");
            Ok(())
        })
        .build();

    let run = host.start("trip", &()).await?;
    match host.result::<()>(run).await {
        Ok(()) => println!("trip booked"),
        Err(e) => println!("trip failed: {e}"),
    }
    Ok(())
}
