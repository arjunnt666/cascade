cascade

I got tired of reading Temporal docs for a thing I wanted to try in one process. so I started this.

You start a run, schedule an activity, complete it, append the finish event, then read the history back. `cascade run` walks that path and fails if the status is not Completed. that is the only slice I will claim.

Core ids, events, and payloads compile (bytes serde included, otherwise the types would be lying). history is in memory. apply_decisions writes ActivityScheduled and WorkflowCompleted into that history.

I have not written disk history, workers in other processes, or real compensations. if you need those, this is the wrong repo today.

```
cargo test --workspace
cargo build -p cascade-cli
./target/debug/cascade run
```

MIT. keep the event log append only.
