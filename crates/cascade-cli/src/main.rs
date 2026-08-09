use clap::{Parser, Subcommand};
use cascade_runtime::RuntimeEngine;
use cascade_core::{Payload, WorkflowOptions};

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
        Commands::Version => {
            println!("cascade 0.1.0");
        }
    }
    Ok(())
}
