# Status

## works today
- replay-based workflows: activities, durable timers, signals, side effects, sagas
- retries with backoff and jitter, per-attempt timeouts
- file-backed history; unfinished runs resume after a restart
- HTTP server, python and js clients
- `cascade run` still walks the low-level API and checks for Completed

## not yet
- more than one process per history (no leases)
- continue-as-new starting a new run
- replay from snapshots
