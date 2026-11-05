mod demo;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;

use anyhow::{bail, Context};
use cascade_core::{ActivityId, ActivityOptions, Payload, WorkflowOptions, WorkflowStatus};
use cascade_history::{FileHistoryStore, HistoryStore};
use cascade_runtime::{Cascade, DecisionKind, RuntimeEngine};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "cascade", about = "durable workflows in one process")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a run on a server started with `cascade serve`.
    Start {
        #[arg(long, default_value = "order")]
        workflow_type: String,
        /// JSON input
        #[arg(long, default_value = "null")]
        input: String,
        #[arg(long, env = "CASCADE_ADDR", default_value = "127.0.0.1:7233")]
        addr: SocketAddr,
    },
    /// Walk one run through the low-level API in memory and print the history.
    Run,
    /// HTTP API with the demo workflows registered.
    Serve {
        #[arg(long, env = "CASCADE_ADDR", default_value = "127.0.0.1:7233")]
        addr: SocketAddr,
        /// Keep history here. In memory if unset.
        #[arg(long, env = "CASCADE_DATA")]
        data: Option<PathBuf>,
    },
    Version,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    match Cli::parse().command {
        Commands::Start {
            workflow_type,
            input,
            addr,
        } => {
            serde_json::from_str::<serde_json::Value>(&input)
                .context("--input is not valid json")?;
            let body = post(addr, &format!("/workflows/{workflow_type}"), &input)?;
            println!("{body}");
        }
        Commands::Run => run_low_level().await?,
        Commands::Serve { addr, data } => {
            let history = match &data {
                Some(dir) => HistoryStore::new(FileHistoryStore::open(dir)?),
                None => HistoryStore::memory(),
            };
            let host = demo::register(Cascade::builder().history(history)).build();
            let resumed = host.recover().await?;
            if resumed > 0 {
                tracing::info!(resumed, "picked up unfinished runs");
            }
            cascade_server::serve(host, addr).await?;
        }
        Commands::Version => println!("cascade {}", env!("CARGO_PKG_VERSION")),
    }
    Ok(())
}

async fn run_low_level() -> anyhow::Result<()> {
    let engine = RuntimeEngine::new();
    let (_wf, run) = engine
        .start(
            "order",
            Payload::from_json(&serde_json::json!({"sku": "demo"}))?,
            WorkflowOptions::default(),
        )
        .await?;
    let act = ActivityId::new();
    let schedule = DecisionKind::ScheduleActivity {
        activity_id: act,
        activity_type: "charge".into(),
        input: Payload::empty(),
        options: ActivityOptions::default(),
    };
    engine.apply_decisions(&run, vec![schedule.into()]).await?;
    engine
        .complete_activity(
            &run,
            act,
            Payload::from_json(&serde_json::json!({"ok": true}))?,
        )
        .await?;
    let done = DecisionKind::CompleteWorkflow {
        result: Payload::from_json(&serde_json::json!({"charged": true}))?,
    };
    engine.apply_decisions(&run, vec![done.into()]).await?;

    let status = engine.get_status(&run).await?;
    println!("status={status:?}");
    for ev in engine.events(&run).await? {
        println!("{} {:?}", ev.sequence, ev.event_type);
    }
    anyhow::ensure!(
        status == WorkflowStatus::Completed,
        "workflow did not complete"
    );
    Ok(())
}

/// Just enough HTTP/1.1 to talk to our own server without pulling in a client.
fn post(addr: SocketAddr, path: &str, body: &str) -> anyhow::Result<String> {
    let mut s = TcpStream::connect(addr)
        .with_context(|| format!("is `cascade serve` running on {addr}?"))?;
    write!(
        s,
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    let mut resp = String::new();
    s.read_to_string(&mut resp)?;
    let (head, body) = resp.split_once("\r\n\r\n").unwrap_or((&resp, ""));
    let status = head.split_whitespace().nth(1).unwrap_or("?");
    if !status.starts_with('2') {
        bail!("server said {status}: {body}");
    }
    Ok(body.to_string())
}
