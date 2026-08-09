# Architecture Overview

Cascade is a durable workflow engine. The source of truth is an append-only event history per run.

## Core pieces

1. **History** (`cascade-history`)  
   Append-only log of events (WorkflowStarted, ActivityScheduled, TimerFired, …).  
   Snapshots allow fast recovery without replaying the entire log.

2. **Runtime** (`cascade-runtime`)  
   Replays history to reconstruct workflow state, then lets the workflow code produce *decisions*.  
   Decisions become new history events + tasks for workers/timers.

3. **Timer service** (`cascade-timer`)  
   Durable timers that fire even after process restarts (when backed by persistent storage).

4. **Worker pool** (`cascade-worker`)  
   Polls activity task queues, executes user code, heartbeats, reports results back into history.

5. **Server** (`cascade-server`)  
   gRPC / HTTP surface for StartWorkflow, Signal, Query, Cancel, etc.

## Determinism

Workflow code must be deterministic with respect to history. All non-deterministic side effects (I/O, time, random) go through the `WorkflowContext` so they are recorded and can be replayed.

## Failure model

- Activities can fail and be retried according to a `RetryPolicy`.
- Workflows can be cancelled or timed out.
- Compensating actions (sagas) are just more activities scheduled by the workflow.

## Status

Most of the interesting machinery is present as traits + in-memory implementations.  
Persistence, leasing, and the actual decision loop are still stubs.
