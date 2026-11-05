# Getting started

```bash
cargo run -p cascade-runtime --example hello
```

that registers a workflow and an activity in-process, runs it, and prints
the history.

## over http

```bash
cargo run -p cascade-cli -- serve --data ./cascade-data
# another shell
cargo run -p cascade-cli -- start --workflow-type order --input '{"sku": "tea"}'
curl localhost:7233/runs/<run_id>
curl localhost:7233/runs/<run_id>/history
```

`approval` waits for a signal:

```bash
cargo run -p cascade-cli -- start --workflow-type approval --input '{"timeout_secs": 600}'
curl -X POST localhost:7233/runs/<run_id>/signals/approve -d '"me"'
```

kill the server while a run is waiting and start it again with the same
`--data`; it picks the run back up.

clients: `packages/cascade-python` (stdlib only) and `packages/cascade-js`
(fetch).
