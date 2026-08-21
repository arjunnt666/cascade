use cascade_core::{ActivityId, ActivityOptions, Payload, WorkflowOptions, WorkflowStatus};
use cascade_runtime::{Decision, DecisionKind, RuntimeEngine};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "cascade", about = "cascade durable workflow tooling")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Start {
        #[arg(long, default_value = "demo")]
        workflow_type: String,
    },
    /// Run a two-step in-process workflow and print the history.
    Run,
    Version,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Start { workflow_type } => {
            let engine = RuntimeEngine::new();
            let (wf, run) = engine
                .start(&workflow_type, Payload::empty(), WorkflowOptions::default())
                .await?;
            println!("started workflow_id={} run_id={}", wf, run);
        }
        Commands::Run => {
            let engine = RuntimeEngine::new();
            let (_wf, run) = engine
                .start(
                    "order",
                    Payload::from_json(&serde_json::json!({"sku": "demo"}))?,
                    WorkflowOptions::default(),
                )
                .await?;
            let act = ActivityId::new();
            engine
                .apply_decisions(
                    &run,
                    vec![Decision {
                        kind: DecisionKind::ScheduleActivity {
                            activity_id: act,
                            activity_type: "charge".into(),
                            input: Payload::empty(),
                            options: ActivityOptions::default(),
                        },
                    }],
                )
                .await?;
            engine
                .complete_activity(
                    &run,
                    act,
                    Payload::from_json(&serde_json::json!({"ok": true}))?,
                )
                .await?;
            engine
                .apply_decisions(
                    &run,
                    vec![Decision {
                        kind: DecisionKind::CompleteWorkflow {
                            result: Payload::from_json(&serde_json::json!({"charged": true}))?,
                        },
                    }],
                )
                .await?;
            println!("status={:?}", engine.get_status(&run).await?);
            for ev in engine.events(&run).await? {
                println!("{} {:?}", ev.sequence, ev.event_type);
            }
            anyhow::ensure!(
                engine.get_status(&run).await? == WorkflowStatus::Completed,
                "workflow did not complete"
            );
        }
        Commands::Version => {
            println!("cascade 0.1.0");
        }
    }
    Ok(())
}
