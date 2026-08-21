# cascade

durable workflow ideas you can actually run in one process.

start a run. append history events. read them back. that is the slice that works today.

not Temporal. not a cluster. an embeddable history log plus a tiny runtime so `cascade start` does something besides print a banner.

## works today

- core ids, events, payloads (bytes serde included, so the types actually compile)
- in-memory history append / read
- `cascade version` and `cascade start --workflow-type demo`
- unit tests on the history store

## does not work yet

- durable disk history
- activity workers across processes
- real compensations / sagas

## try it

```bash
cargo test --workspace
cargo build -p cascade-cli
./target/debug/cascade version
./target/debug/cascade start --workflow-type demo
```

## license

mit. keep the event log append-only.
