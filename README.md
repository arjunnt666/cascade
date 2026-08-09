# cascade

lightweight durable workflow engine.

workflows as code. activity retries. event history that doesn’t vanish when the process dies. timers that actually fire later. compensations when things go sideways.

embed it in your binary or run the server and yell at it over grpc/http.

## why

most “orchestration” tools want a cluster, a database you babysit, and a phd in their yaml dialect.  
cascade tries to stay small enough that you can actually read the source without crying.

not production-hardened. not battle-tested. not the thing you put money on yet.  
but the bones are there if you want to poke them.

## quick feel

```rust
// rough sketch — real api still settling
#[cascade::workflow]
async fn order_flow(ctx: Context, order_id: String) -> Result<()> {
    let payment = ctx.activity(charge_card, order_id.clone()).await?;
    let ship = ctx.activity(ship_it, order_id).await?;
    Ok(())
}
```

activities can retry, heartbeat, and fail independently.  
history is append-only. you can replay.

## status

early. lots of stubs. the interesting parts (history log, timer wheel, worker leasing) are sketched.  
don’t file a haunted bug report at 3am unless you’re prepared for silence.

## crates

- `cascade-core` — types, events, errors
- `cascade-history` — durable event log + snapshots
- `cascade-runtime` — workflow interpreter + dispatcher
- `cascade-timer` — durable timers
- `cascade-worker` — activity workers + heartbeats
- `cascade-server` — grpc + http surface
- `cascade-cli` — local tooling
- `cascade-ffi` — c/python/js glue

js + python packages live under `packages/`.

## license

mit. do what you want. don’t sue me.
