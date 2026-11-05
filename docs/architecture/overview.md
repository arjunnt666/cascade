# Architecture

One append-only event log per run. Everything else is derived from it.

## crates

- `cascade-core`: ids, events, payloads, options, errors.
- `cascade-history`: the log. `MemoryHistoryStore` and `FileHistoryStore`
  (one JSON-lines file per run, fsync per append, a torn last line is cut
  off on open). Appends must carry the next sequence number, so two writers
  can't both extend a run.
- `cascade-runtime`: `WorkflowContext` (replay), `Cascade` (the host that
  drives runs), `RuntimeEngine` (append decisions by hand).
- `cascade-worker`: activity registry and a bounded worker pool.
- `cascade-timer`: a min-heap of timers with cancel. The host doesn't need
  it (it sleeps per timer), it's there for callers with lots of timers.
- `cascade-server`: HTTP API.
- `cascade-cli`: `run`, `serve`, `start`, `version`.

## how a run moves

1. `start` appends `WorkflowStarted` and drives the run.
2. driving = load history, run the workflow function from the top against
   it. each `ctx.activity` / `ctx.sleep` / `ctx.signal` / `ctx.side_effect`
   takes the next slot of its kind. if history has the slot, its recorded
   outcome is used; otherwise a decision is recorded and the future stays
   pending.
3. new decisions are appended (`ActivityScheduled`, `TimerStarted`,
   `MarkerRecorded`, or `WorkflowCompleted` / `WorkflowFailed`).
4. activities and timers with no outcome get a tokio task. when one
   finishes it appends its result and drives the run again.

drives for one run are serialized by a per-run lock.

## determinism

outcomes are revealed to the workflow in history order: one poll per
outcome event. so `select!` over a timer and a signal picks whichever was
recorded first, on every replay, even though both are in history later.

if replay asks for activity "b" where history has "a" in that slot, the run
fails with a non-determinism error rather than guessing.

`ctx.now()` is the timestamp of the newest event replay has reached, not
the wall clock.

## not done

- one process per history. no leases, so two servers on the same data dir
  will corrupt runs.
- continue-as-new is an event type but nothing starts the new run.
- snapshots are stored but replay always starts from event 0.
