//! In-process host: register workflows and activities, start runs, and let
//! it push them to completion.
//!
//! Everything durable lives in history. In-flight activity and timer tasks
//! are just tokio tasks; if the process dies they are gone, and `recover()`
//! on the next start re-spawns whatever history says is still outstanding.
//! That means an activity can run more than once. Make them idempotent.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use cascade_core::{
    ActivityId, ActivityOptions, CascadeError, EventPayload, HistoryEvent, Payload, Result, RunId,
    TimerId, WorkflowOptions, WorkflowStatus,
};
use cascade_history::HistoryStore;
use cascade_worker::{ActivityError, ActivityExecutor, ActivityRegistry};
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{Notify, Semaphore};
use tracing::{debug, warn};

use crate::context::{ReplayState, WorkflowContext};
use crate::decision::DecisionKind;
use crate::engine::{status_of, RuntimeEngine};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type WorkflowFn = Arc<dyn Fn(WorkflowContext, Payload) -> BoxFuture<Result<Payload>> + Send + Sync>;

pub struct CascadeBuilder {
    history: HistoryStore,
    workflows: HashMap<String, WorkflowFn>,
    activities: ActivityRegistry,
    max_concurrent_activities: usize,
}

impl CascadeBuilder {
    pub fn history(mut self, history: HistoryStore) -> Self {
        self.history = history;
        self
    }

    pub fn max_concurrent_activities(mut self, n: usize) -> Self {
        self.max_concurrent_activities = n.max(1);
        self
    }

    pub fn workflow<I, O, F, Fut>(mut self, name: &str, f: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + 'static,
        F: Fn(WorkflowContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        let f = Arc::new(f);
        let wrapped: WorkflowFn = Arc::new(move |ctx, payload: Payload| {
            let f = f.clone();
            Box::pin(async move {
                let input: I = payload.as_json()?;
                let out = f(ctx, input).await?;
                Ok(Payload::from_json(&out)?)
            })
        });
        self.workflows.insert(name.to_string(), wrapped);
        self
    }

    pub fn activity<I, O, F, Fut>(mut self, name: &str, f: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<O, ActivityError>> + Send + 'static,
    {
        self.activities.register(name, f);
        self
    }

    pub fn build(self) -> Cascade {
        Cascade {
            inner: Arc::new(Inner {
                engine: RuntimeEngine::with_history(self.history),
                workflows: self.workflows,
                activities: self.activities,
                permits: Semaphore::new(self.max_concurrent_activities),
                locks: Mutex::new(HashMap::new()),
                in_flight: Mutex::new(HashSet::new()),
                changed: Notify::new(),
            }),
        }
    }
}

#[derive(Clone)]
pub struct Cascade {
    inner: Arc<Inner>,
}

struct Inner {
    engine: RuntimeEngine,
    workflows: HashMap<String, WorkflowFn>,
    activities: ActivityRegistry,
    permits: Semaphore,
    locks: Mutex<HashMap<RunId, Arc<tokio::sync::Mutex<()>>>>,
    /// Activity and timer ids that have a live task in this process.
    in_flight: Mutex<HashSet<uuid::Uuid>>,
    changed: Notify,
}

impl Cascade {
    pub fn builder() -> CascadeBuilder {
        CascadeBuilder {
            history: HistoryStore::memory(),
            workflows: HashMap::new(),
            activities: ActivityRegistry::new(),
            max_concurrent_activities: 64,
        }
    }

    pub fn workflow_types(&self) -> Vec<&str> {
        let mut v: Vec<_> = self.inner.workflows.keys().map(String::as_str).collect();
        v.sort_unstable();
        v
    }

    pub async fn start<I: Serialize>(&self, workflow_type: &str, input: &I) -> Result<RunId> {
        self.start_payload(workflow_type, Payload::from_json(input)?)
            .await
    }

    pub async fn start_payload(&self, workflow_type: &str, input: Payload) -> Result<RunId> {
        if !self.inner.workflows.contains_key(workflow_type) {
            return Err(CascadeError::UnknownWorkflow(workflow_type.to_string()));
        }
        let (_wf, run) = self
            .inner
            .engine
            .start(workflow_type, input, WorkflowOptions::default())
            .await?;
        self.inner.drive(run).await?;
        Ok(run)
    }

    pub async fn status(&self, run: RunId) -> Result<WorkflowStatus> {
        self.inner.engine.get_status(&run).await
    }

    pub async fn history(&self, run: RunId) -> Result<Vec<HistoryEvent>> {
        self.inner.engine.events(&run).await
    }

    pub async fn signal<I: Serialize>(&self, run: RunId, name: &str, input: &I) -> Result<()> {
        let payload = EventPayload::SignalReceived {
            signal_name: name.to_string(),
            input: Payload::from_json(input)?,
        };
        self.inner.append_locked(run, vec![payload]).await?;
        self.inner.drive(run).await
    }

    /// Ends the run right away. Running activities finish but their results
    /// are dropped.
    pub async fn cancel(&self, run: RunId, reason: &str) -> Result<()> {
        let payload = EventPayload::WorkflowCancelled {
            reason: reason.to_string(),
        };
        self.inner.append_locked(run, vec![payload]).await?;
        self.inner.changed.notify_waiters();
        Ok(())
    }

    /// Wait for the run to finish and decode its result.
    pub async fn result<O: DeserializeOwned>(&self, run: RunId) -> Result<O> {
        Ok(self.result_payload(run).await?.as_json()?)
    }

    pub async fn result_payload(&self, run: RunId) -> Result<Payload> {
        loop {
            let notified = self.inner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let events = self.inner.engine.events(&run).await?;
            if events.is_empty() {
                return Err(CascadeError::RunNotFound(run.to_string()));
            }
            if let Some(end) = events.iter().rev().find(|e| e.is_terminal()) {
                return match &end.payload {
                    EventPayload::WorkflowCompleted { result } => Ok(result.clone()),
                    EventPayload::WorkflowFailed { reason } => {
                        Err(CascadeError::WorkflowFailed(reason.clone()))
                    }
                    EventPayload::WorkflowContinuedAsNew { .. } => Err(
                        CascadeError::InvalidTransition("run continued as new".into()),
                    ),
                    _ => Err(CascadeError::Cancelled),
                };
            }
            notified.await;
        }
    }

    /// Pick up every unfinished run in history. Call once after building
    /// on top of a durable store. Returns how many runs were resumed.
    pub async fn recover(&self) -> Result<usize> {
        let mut resumed = 0;
        for run in self.inner.engine.history().list_runs().await? {
            let events = self.inner.engine.events(&run).await?;
            if !events.is_empty() && status_of(&events) == WorkflowStatus::Running {
                self.inner.drive(run).await?;
                resumed += 1;
            }
        }
        Ok(resumed)
    }
}

struct Outstanding {
    activities: Vec<(ActivityId, String, Payload, ActivityOptions, u32)>,
    timers: Vec<(TimerId, DateTime<Utc>)>,
}

impl Inner {
    fn run_lock(&self, run: RunId) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(run)
            .or_default()
            .clone()
    }

    async fn append_locked(&self, run: RunId, payloads: Vec<EventPayload>) -> Result<()> {
        let lock = self.run_lock(run);
        let _held = lock.lock().await;
        self.engine.append(&run, payloads).await
    }

    fn claim(&self, id: uuid::Uuid) -> bool {
        self.in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id)
    }

    fn release(&self, id: uuid::Uuid) {
        self.in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
    }

    /// Replay the run, write down any new decisions, and spawn work for
    /// whatever is still outstanding.
    fn drive(self: &Arc<Self>, run: RunId) -> BoxFuture<Result<()>> {
        let this = self.clone();
        Box::pin(async move {
            let lock = this.run_lock(run);
            let _held = lock.lock().await;

            let events = this.engine.events(&run).await?;
            let Some(first) = events.first() else {
                return Err(CascadeError::RunNotFound(run.to_string()));
            };
            if events.iter().any(HistoryEvent::is_terminal) {
                return Ok(());
            }
            let EventPayload::WorkflowStarted {
                workflow_type,
                input,
                ..
            } = &first.payload
            else {
                return Err(CascadeError::Internal(format!(
                    "run {run} has no start event"
                )));
            };

            let (decisions, outstanding) = match this.workflows.get(workflow_type) {
                Some(wf) => replay(wf, &events, input.clone())?,
                None => (
                    vec![DecisionKind::FailWorkflow {
                        reason: format!("no workflow registered as {workflow_type:?}"),
                    }],
                    None,
                ),
            };

            if !decisions.is_empty() {
                debug!(%run, count = decisions.len(), "applying decisions");
                let payloads = decisions
                    .into_iter()
                    .map(DecisionKind::into_payload)
                    .collect();
                this.engine.append(&run, payloads).await?;
            }
            if let Some(out) = outstanding {
                this.spawn_outstanding(run, out);
            }
            this.changed.notify_waiters();
            Ok(())
        })
    }

    fn spawn_outstanding(self: &Arc<Self>, run: RunId, out: Outstanding) {
        for (id, ty, input, options, failed) in out.activities {
            if self.claim(id.0) {
                tokio::spawn(
                    self.clone()
                        .run_activity(run, id, ty, input, options, failed + 1),
                );
            }
        }
        for (id, fire_at) in out.timers {
            if self.claim(id.0) {
                let this = self.clone();
                tokio::spawn(async move {
                    let wait = (fire_at - Utc::now()).to_std().unwrap_or_default();
                    tokio::time::sleep(wait).await;
                    // Fails if the run ended meanwhile, which is fine.
                    let fired = this
                        .append_locked(run, vec![EventPayload::TimerFired { timer_id: id }])
                        .await;
                    this.release(id.0);
                    if fired.is_ok() {
                        if let Err(e) = this.drive(run).await {
                            warn!(%run, error = %e, "drive after timer failed");
                        }
                    }
                });
            }
        }
    }

    async fn run_activity(
        self: Arc<Self>,
        run: RunId,
        id: ActivityId,
        activity_type: String,
        input: Payload,
        options: ActivityOptions,
        mut attempt: u32,
    ) {
        let policy = options.retry_policy.clone();
        let timeout = options.start_to_close_timeout.and_then(|t| t.to_std().ok());
        loop {
            let permit = self
                .permits
                .acquire()
                .await
                .expect("semaphore never closed");
            let started = EventPayload::ActivityStarted {
                activity_id: id,
                attempt,
            };
            if self.append_locked(run, vec![started]).await.is_err() {
                break; // run is over
            }
            let call = self.activities.execute(&activity_type, input.clone());
            let res = match timeout {
                Some(t) => tokio::time::timeout(t, call).await.unwrap_or_else(|_| {
                    Err(ActivityError::retryable(format!("timed out after {t:?}")))
                }),
                None => call.await,
            };
            drop(permit);

            let (event, again) = match res {
                Ok(result) => (
                    EventPayload::ActivityCompleted {
                        activity_id: id,
                        result,
                    },
                    false,
                ),
                Err(e) => {
                    let retry = !e.non_retryable && policy.should_retry(attempt, &e.message);
                    debug!(%run, activity = %activity_type, attempt, retry, error = %e, "activity failed");
                    let event = EventPayload::ActivityFailed {
                        activity_id: id,
                        reason: e.message,
                        attempt,
                        will_retry: retry,
                    };
                    (event, retry)
                }
            };
            if self.append_locked(run, vec![event]).await.is_err() || !again {
                break;
            }
            tokio::time::sleep(policy.backoff(attempt, fastrand::f64())).await;
            attempt += 1;
        }
        self.release(id.0);
        if let Err(e) = self.drive(run).await {
            warn!(%run, error = %e, "drive after activity failed");
        }
    }
}

/// Run workflow code against `events`. Returns the decisions to append, and
/// the outstanding work if the run is still going.
fn replay(
    wf: &WorkflowFn,
    events: &[HistoryEvent],
    input: Payload,
) -> Result<(Vec<DecisionKind>, Option<Outstanding>)> {
    let state = Arc::new(Mutex::new(ReplayState::from_history(events)?));
    let lock = || state.lock().unwrap_or_else(|e| e.into_inner());
    let mut fut = wf(WorkflowContext::new(state.clone()), input);
    let mut cx = Context::from_waker(Waker::noop());

    // Poll once with only the start visible, then once per outcome in
    // history order, then once with everything visible.
    let mut steps: Vec<Option<usize>> = vec![None];
    steps.extend(lock().boundaries().into_iter().map(Some));
    steps.push(Some(events.len() - 1));

    let mut finished = None;
    for step in steps {
        if let Some(i) = step {
            lock().reveal_up_to(i);
        }
        if let Poll::Ready(r) = fut.as_mut().poll(&mut cx) {
            finished = Some(r);
            break;
        }
        if lock().error.is_some() {
            break;
        }
    }
    drop(fut);

    let mut st = lock();
    if let Some(err) = st.error.take() {
        // Can't trust anything this replay decided.
        return Ok((
            vec![DecisionKind::FailWorkflow {
                reason: err.to_string(),
            }],
            None,
        ));
    }
    let mut decisions = std::mem::take(&mut st.decisions);
    match finished {
        Some(Ok(result)) => decisions.push(DecisionKind::CompleteWorkflow { result }),
        Some(Err(e)) => decisions.push(DecisionKind::FailWorkflow {
            reason: e.to_string(),
        }),
        None => {
            let out = Outstanding {
                activities: st
                    .activities
                    .iter()
                    .filter(|a| a.outcome.is_none())
                    .map(|a| {
                        (
                            a.id,
                            a.activity_type.clone(),
                            a.input.clone(),
                            a.options.clone(),
                            a.failed_attempts,
                        )
                    })
                    .collect(),
                timers: st
                    .timers
                    .iter()
                    .filter(|t| t.fired_at.is_none())
                    .map(|t| (t.id, t.fire_at))
                    .collect(),
            };
            return Ok((decisions, Some(out)));
        }
    }
    Ok((decisions, None))
}
