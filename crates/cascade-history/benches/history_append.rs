//! Append throughput for both backends.
//!
//!     cargo bench -p cascade-history
//!
//! Plain timing loop, no criterion. Good enough to notice a 2x regression,
//! not for publishing numbers.

use std::time::Instant;

use cascade_core::{EventPayload, HistoryEvent, Payload, RunId, WorkflowId};
use cascade_history::{FileHistoryStore, HistoryBackend, MemoryHistoryStore};

fn event(run: RunId, wf: WorkflowId, seq: u64) -> HistoryEvent {
    HistoryEvent::new(
        run,
        wf,
        seq,
        EventPayload::MarkerRecorded {
            name: "bench".into(),
            value: Payload::from_bytes(vec![0u8; 64]),
        },
    )
}

async fn bench(name: &str, store: &dyn HistoryBackend, n: u64, batch: u64) {
    let (run, wf) = (RunId::new(), WorkflowId::new());
    let start = Instant::now();
    let mut seq = 0;
    while seq < n {
        let events = (seq..seq + batch).map(|s| event(run, wf, s)).collect();
        store.append(&run, events).await.unwrap();
        seq += batch;
    }
    let secs = start.elapsed().as_secs_f64();
    println!(
        "{name:<28} {n:>7} events  batch {batch:>3}  {:>10.0} events/s  {:>8.1} us/append",
        n as f64 / secs,
        secs * 1e6 / (n / batch) as f64
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mem = MemoryHistoryStore::new();
    bench("memory", &mem, 200_000, 1).await;
    bench("memory", &mem, 200_000, 50).await;

    let dir = tempfile::tempdir().unwrap();
    let file = FileHistoryStore::open(dir.path()).unwrap();
    bench("file (fsync per append)", &file, 2_000, 1).await;
    bench("file (fsync per append)", &file, 20_000, 50).await;
}
