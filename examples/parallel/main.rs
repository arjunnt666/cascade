//! Fan out, then join. cargo run -p cascade-runtime --example parallel

use std::time::{Duration, Instant};

use cascade_runtime::{Cascade, WorkflowContext};

#[tokio::main]
async fn main() -> cascade_core::Result<()> {
    let host = Cascade::builder()
        .workflow(
            "sizes",
            |ctx: WorkflowContext, urls: Vec<String>| async move {
                // All three get scheduled in the same decision batch.
                let (a, b, c) = tokio::join!(
                    ctx.activity::<_, usize>("fetch", &urls[0]),
                    ctx.activity::<_, usize>("fetch", &urls[1]),
                    ctx.activity::<_, usize>("fetch", &urls[2]),
                );
                Ok(a? + b? + c?)
            },
        )
        .activity("fetch", |url: String| async move {
            // pretend to download
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(url.len())
        })
        .build();

    let t = Instant::now();
    let urls = vec![
        "https://a.example".to_string(),
        "https://bb.example".into(),
        "https://ccc.example".into(),
    ];
    let run = host.start("sizes", &urls).await?;
    let total: usize = host.result(run).await?;
    println!(
        "total {total} in {:?} (one activity takes 200ms)",
        t.elapsed()
    );
    Ok(())
}
