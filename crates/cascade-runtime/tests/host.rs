use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cascade_core::{ActivityOptions, CascadeError, EventType, RetryPolicy, WorkflowStatus};
use cascade_history::{FileHistoryStore, HistoryStore};
use cascade_runtime::{ActivityError, Cascade, Saga, WorkflowContext};
use serde_json::{json, Value};

fn quick_retries(max: u32) -> ActivityOptions {
    ActivityOptions {
        retry_policy: RetryPolicy {
            initial_interval: chrono::Duration::milliseconds(5),
            maximum_interval: chrono::Duration::milliseconds(20),
            maximum_attempts: max,
            ..RetryPolicy::default()
        },
        ..ActivityOptions::default()
    }
}

async fn order(ctx: WorkflowContext, sku: String) -> cascade_core::Result<Value> {
    let price: u32 = ctx.activity("price", &sku).await?;
    let receipt: String = ctx.activity("charge", &price).await?;
    Ok(json!({ "sku": sku, "price": price, "receipt": receipt }))
}

fn order_host() -> Cascade {
    Cascade::builder()
        .workflow("order", order)
        .activity(
            "price",
            |sku: String| async move { Ok(sku.len() as u32 * 100) },
        )
        .activity("charge", |amount: u32| async move {
            Ok(format!("rcpt-{amount}"))
        })
        .build()
}

#[tokio::test]
async fn two_activities_in_sequence() {
    let host = order_host();
    let run = host.start("order", &"abc").await.unwrap();
    let out: Value = host.result(run).await.unwrap();
    assert_eq!(
        out,
        json!({"sku": "abc", "price": 300, "receipt": "rcpt-300"})
    );

    let kinds: Vec<_> = host
        .history(run)
        .await
        .unwrap()
        .iter()
        .map(|e| e.event_type)
        .collect();
    use EventType::*;
    assert_eq!(
        kinds,
        [
            WorkflowStarted,
            ActivityScheduled,
            ActivityStarted,
            ActivityCompleted,
            ActivityScheduled,
            ActivityStarted,
            ActivityCompleted,
            WorkflowCompleted
        ]
    );
}

#[tokio::test]
async fn unknown_workflow_is_rejected_up_front() {
    let host = order_host();
    assert!(matches!(
        host.start("nope", &()).await,
        Err(CascadeError::UnknownWorkflow(_))
    ));
}

#[tokio::test]
async fn flaky_activity_retries_then_succeeds() {
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let host = Cascade::builder()
        .workflow("w", |ctx: WorkflowContext, (): ()| async move {
            ctx.activity_with::<_, u32>("flaky", &(), quick_retries(5))
                .await
        })
        .activity("flaky", move |(): ()| {
            let c = c.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    Err(ActivityError::retryable("not yet"))
                } else {
                    Ok(n)
                }
            }
        })
        .build();
    let run = host.start("w", &()).await.unwrap();
    assert_eq!(host.result::<u32>(run).await.unwrap(), 3);
    let fails = host
        .history(run)
        .await
        .unwrap()
        .iter()
        .filter(|e| e.event_type == EventType::ActivityFailed)
        .count();
    assert_eq!(fails, 2);
}

#[tokio::test]
async fn fatal_error_skips_retries_and_fails_the_run() {
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let host = Cascade::builder()
        .workflow("w", |ctx: WorkflowContext, (): ()| async move {
            ctx.activity_with::<_, ()>("bad", &(), quick_retries(5))
                .await
        })
        .activity("bad", move |(): ()| {
            c.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(ActivityError::fatal("card declined")) }
        })
        .build();
    let run = host.start("w", &()).await.unwrap();
    let err = host.result::<()>(run).await.unwrap_err();
    assert!(err.to_string().contains("card declined"), "{err}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(host.status(run).await.unwrap(), WorkflowStatus::Failed);
}

#[tokio::test]
async fn timer_then_signal() {
    let host = Cascade::builder()
        .workflow("approve", |ctx: WorkflowContext, (): ()| async move {
            ctx.sleep(Duration::from_millis(20)).await?;
            let who: String = ctx.signal("approved_by").await?;
            Ok(format!("approved by {who}"))
        })
        .build();
    let run = host.start("approve", &()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(host.status(run).await.unwrap(), WorkflowStatus::Running);
    host.signal(run, "approved_by", &"arjun").await.unwrap();
    assert_eq!(
        host.result::<String>(run).await.unwrap(),
        "approved by arjun"
    );
}

#[tokio::test]
async fn select_picks_the_same_winner_on_replay() {
    // Signal arrives before the timer fires. Later replays see the timer
    // fired too, but must still take the signal branch.
    let host = Cascade::builder()
        .workflow("race", |ctx: WorkflowContext, (): ()| async move {
            let winner = tokio::select! {
                biased;
                _ = ctx.sleep(Duration::from_millis(30)) => "timer",
                _ = ctx.signal::<()>("go") => "signal",
            };
            ctx.activity::<_, ()>("note", &winner).await?;
            Ok(winner.to_string())
        })
        .activity("note", |_: String| async {
            tokio::time::sleep(Duration::from_millis(60)).await;
            Ok(())
        })
        .build();
    let run = host.start("race", &()).await.unwrap();
    host.signal(run, "go", &()).await.unwrap();
    assert_eq!(host.result::<String>(run).await.unwrap(), "signal");
}

#[tokio::test]
async fn side_effects_are_recorded_once() {
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let host = Cascade::builder()
        .workflow("w", move |ctx: WorkflowContext, (): ()| {
            let c = c.clone();
            async move {
                let n = ctx.side_effect(|| c.fetch_add(1, Ordering::SeqCst) + 41)?;
                ctx.activity::<_, ()>("noop", &()).await?;
                Ok(n)
            }
        })
        .activity("noop", |(): ()| async { Ok(()) })
        .build();
    let run = host.start("w", &()).await.unwrap();
    assert_eq!(host.result::<u32>(run).await.unwrap(), 41);
    // Replayed at least twice, closure ran once.
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn saga_compensates_in_reverse() {
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let step = |log: Arc<std::sync::Mutex<Vec<String>>>| {
        move |name: String| {
            let log = log.clone();
            async move {
                log.lock().unwrap().push(name);
                Ok(())
            }
        }
    };
    let host = Cascade::builder()
        .workflow("trip", |ctx: WorkflowContext, (): ()| async move {
            let mut saga = Saga::new(&ctx);
            ctx.activity::<_, ()>("do", &"flight").await?;
            saga.add("undo", &"flight")?;
            ctx.activity::<_, ()>("do", &"hotel").await?;
            saga.add("undo", &"hotel")?;
            if let Err(e) = ctx
                .activity_with::<_, ()>("car", &(), quick_retries(1))
                .await
            {
                saga.compensate().await;
                return Err(e);
            }
            Ok(())
        })
        .activity("do", step(log.clone()))
        .activity("undo", {
            let log = log.clone();
            move |name: String| {
                let log = log.clone();
                async move {
                    log.lock().unwrap().push(format!("undo {name}"));
                    Ok(())
                }
            }
        })
        .activity("car", |(): ()| async {
            Err::<(), _>(ActivityError::retryable("no cars"))
        })
        .build();
    let run = host.start("trip", &()).await.unwrap();
    assert!(host.result::<()>(run).await.is_err());
    assert_eq!(
        *log.lock().unwrap(),
        ["flight", "hotel", "undo hotel", "undo flight"]
    );
}

#[tokio::test]
async fn cancel_ends_the_run() {
    let host = Cascade::builder()
        .workflow("w", |ctx: WorkflowContext, (): ()| async move {
            ctx.signal::<()>("never").await
        })
        .build();
    let run = host.start("w", &()).await.unwrap();
    host.cancel(run, "changed my mind").await.unwrap();
    assert!(matches!(
        host.result::<()>(run).await,
        Err(CascadeError::Cancelled)
    ));
    assert!(host.signal(run, "never", &()).await.is_err());
}

#[tokio::test]
async fn changed_code_is_caught_as_nondeterminism() {
    let dir = tempfile::tempdir().unwrap();
    let store = || HistoryStore::new(FileHistoryStore::open(dir.path()).unwrap());

    let v1 = Cascade::builder()
        .history(store())
        .workflow("w", |ctx: WorkflowContext, (): ()| async move {
            ctx.activity::<_, ()>("a", &()).await?;
            ctx.signal::<()>("next").await
        })
        .activity("a", |(): ()| async { Ok(()) })
        .build();
    let run = v1.start("w", &()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    drop(v1);

    let v2 = Cascade::builder()
        .history(store())
        .workflow("w", |ctx: WorkflowContext, (): ()| async move {
            ctx.activity::<_, ()>("b", &()).await?;
            ctx.signal::<()>("next").await
        })
        .build();
    assert_eq!(v2.recover().await.unwrap(), 1);
    let err = v2.result::<()>(run).await.unwrap_err();
    assert!(err.to_string().contains("non-deterministic"), "{err}");
}

#[tokio::test]
async fn recover_picks_up_where_a_dead_process_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let store = || HistoryStore::new(FileHistoryStore::open(dir.path()).unwrap());
    let charged = Arc::new(AtomicU32::new(0));

    let build = |charged: Arc<AtomicU32>| {
        Cascade::builder()
            .history(store())
            .workflow("order", order)
            .activity("price", |sku: String| async move { Ok(sku.len() as u32) })
            .activity("charge", move |amount: u32| {
                charged.fetch_add(1, Ordering::SeqCst);
                async move { Ok(format!("rcpt-{amount}")) }
            })
    };

    // First process gets through price, then hangs in charge and "dies".
    let first = Cascade::builder()
        .history(store())
        .workflow("order", order)
        .activity("price", |sku: String| async move { Ok(sku.len() as u32) })
        .activity("charge", |_: u32| async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(String::new())
        })
        .build();
    let run = first.start("order", &"abcd").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(first.status(run).await.unwrap(), WorkflowStatus::Running);
    drop(first);

    let second = build(charged.clone()).build();
    assert_eq!(second.recover().await.unwrap(), 1);
    let out: Value = second.result(run).await.unwrap();
    assert_eq!(out["receipt"], "rcpt-4");
    assert_eq!(charged.load(Ordering::SeqCst), 1);
}
