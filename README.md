# cascade

durable workflow ideas you can actually run in one process.

start a run. schedule an activity. complete it. append the finish event. read the history back. that is the slice that works today.

not Temporal. not a cluster. an embeddable history log plus a tiny runtime so `cascade run` does something besides print a banner.

## works today

- core ids, events, payloads (bytes serde included, so the types actually compile)
- in-memory history append / read
- apply_decisions writes ActivityScheduled / WorkflowCompleted into history
- `cascade run` walks start -> activity -> complete and refuses to succeed if status is not Completed

## does not work yet

- durable disk history
- activity workers across processes
- real compensations / sagas

## try it

```bash
cargo test --workspace
cargo build -p cascade-cli
./target/debug/cascade run
```

## license

mit. keep the event log append-only.
