//! cargo run -p cascade-runtime --example hello

use cascade_runtime::{Cascade, WorkflowContext};

#[tokio::main]
async fn main() -> cascade_core::Result<()> {
    let host = Cascade::builder()
        .workflow("hello", |ctx: WorkflowContext, name: String| async move {
            let greeting: String = ctx.activity("greet", &name).await?;
            Ok(greeting)
        })
        .activity("greet", |name: String| async move {
            Ok(format!("hello, {name}"))
        })
        .build();

    let run = host.start("hello", &"arjun").await?;
    println!("{}", host.result::<String>(run).await?);
    for e in host.history(run).await? {
        println!("  {:>2} {:?}", e.sequence, e.event_type);
    }
    Ok(())
}
