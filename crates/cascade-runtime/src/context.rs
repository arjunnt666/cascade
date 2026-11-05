//! Workflow-side API.
//!
//! Workflow functions are re-executed from the top every time the run moves
//! forward. Each call on the context (activity, sleep, signal, side effect)
//! takes the next ordinal slot of its kind. If history already has that
//! slot, the recorded outcome is returned; if not, a decision is recorded
//! and the future stays pending until the host drives the run again.
//!
//! Outcomes are revealed in history order (see `Replay::reveal_up_to`), so
//! code that races two futures picks the same winner on every replay.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use cascade_core::{
    ActivityId, ActivityOptions, CascadeError, EventPayload, HistoryEvent, Payload, Result, RunId,
    TimerId, WorkflowId,
};
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};

use crate::decision::DecisionKind;

pub(crate) struct ActivitySlot {
    pub id: ActivityId,
    pub activity_type: String,
    pub input: Payload,
    pub options: ActivityOptions,
    /// Index into history of the event that settled it, plus the outcome.
    pub outcome: Option<(usize, std::result::Result<Payload, String>)>,
    pub failed_attempts: u32,
}

pub(crate) struct TimerSlot {
    pub id: TimerId,
    pub fire_at: DateTime<Utc>,
    pub fired_at: Option<usize>,
}

struct Signal {
    name: String,
    input: Payload,
    at: usize,
}

pub(crate) struct ReplayState {
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    pub activities: Vec<ActivitySlot>,
    pub timers: Vec<TimerSlot>,
    markers: Vec<Payload>,
    signals: Vec<Signal>,
    timestamps: Vec<DateTime<Utc>>,

    /// Outcomes recorded at history index < visible can be observed.
    visible: usize,
    next_activity: usize,
    next_timer: usize,
    next_marker: usize,
    signal_cursor: HashMap<String, usize>,

    pub decisions: Vec<DecisionKind>,
    pub error: Option<CascadeError>,
    wakers: Vec<Waker>,
}

impl ReplayState {
    pub(crate) fn from_history(events: &[HistoryEvent]) -> Result<Self> {
        let first = events
            .first()
            .ok_or_else(|| CascadeError::Internal("empty history".into()))?;
        let mut st = ReplayState {
            run_id: first.run_id,
            workflow_id: first.workflow_id,
            activities: Vec::new(),
            timers: Vec::new(),
            markers: Vec::new(),
            signals: Vec::new(),
            timestamps: events.iter().map(|e| e.timestamp).collect(),
            visible: 1,
            next_activity: 0,
            next_timer: 0,
            next_marker: 0,
            signal_cursor: HashMap::new(),
            decisions: Vec::new(),
            error: None,
            wakers: Vec::new(),
        };
        let mut by_activity: HashMap<ActivityId, usize> = HashMap::new();
        let mut by_timer: HashMap<TimerId, usize> = HashMap::new();
        for (i, e) in events.iter().enumerate() {
            match &e.payload {
                EventPayload::ActivityScheduled {
                    activity_id,
                    activity_type,
                    input,
                    options,
                } => {
                    by_activity.insert(*activity_id, st.activities.len());
                    st.activities.push(ActivitySlot {
                        id: *activity_id,
                        activity_type: activity_type.clone(),
                        input: input.clone(),
                        options: options.clone(),
                        outcome: None,
                        failed_attempts: 0,
                    });
                }
                EventPayload::ActivityCompleted {
                    activity_id,
                    result,
                } => {
                    if let Some(&s) = by_activity.get(activity_id) {
                        st.activities[s].outcome = Some((i, Ok(result.clone())));
                    }
                }
                EventPayload::ActivityFailed {
                    activity_id,
                    reason,
                    will_retry,
                    ..
                } => {
                    if let Some(&s) = by_activity.get(activity_id) {
                        let slot = &mut st.activities[s];
                        slot.failed_attempts += 1;
                        if !will_retry {
                            slot.outcome = Some((i, Err(reason.clone())));
                        }
                    }
                }
                EventPayload::TimerStarted { timer_id, fire_at } => {
                    by_timer.insert(*timer_id, st.timers.len());
                    st.timers.push(TimerSlot {
                        id: *timer_id,
                        fire_at: *fire_at,
                        fired_at: None,
                    });
                }
                EventPayload::TimerFired { timer_id } => {
                    if let Some(&s) = by_timer.get(timer_id) {
                        st.timers[s].fired_at.get_or_insert(i);
                    }
                }
                EventPayload::MarkerRecorded { value, .. } => st.markers.push(value.clone()),
                EventPayload::SignalReceived { signal_name, input } => st.signals.push(Signal {
                    name: signal_name.clone(),
                    input: input.clone(),
                    at: i,
                }),
                _ => {}
            }
        }
        Ok(st)
    }

    /// History indices where workflow code could observe something new.
    pub(crate) fn boundaries(&self) -> Vec<usize> {
        let mut b: Vec<usize> = self
            .activities
            .iter()
            .filter_map(|a| a.outcome.as_ref().map(|o| o.0))
            .chain(self.timers.iter().filter_map(|t| t.fired_at))
            .chain(self.signals.iter().map(|s| s.at))
            .collect();
        b.sort_unstable();
        b.dedup();
        b
    }

    pub(crate) fn reveal_up_to(&mut self, index: usize) {
        self.visible = index + 1;
        for w in self.wakers.drain(..) {
            w.wake();
        }
    }

    fn fail(&mut self, err: CascadeError) -> CascadeError {
        let msg = err.to_string();
        self.error
            .get_or_insert(CascadeError::NonDeterminism(msg.clone()));
        CascadeError::NonDeterminism(msg)
    }

    fn now(&self) -> DateTime<Utc> {
        let i = self.visible.min(self.timestamps.len()).saturating_sub(1);
        self.timestamps[i]
    }
}

/// Handle given to workflow code. Cheap to clone; clones share state.
#[derive(Clone)]
pub struct WorkflowContext {
    state: Arc<Mutex<ReplayState>>,
}

impl WorkflowContext {
    pub(crate) fn new(state: Arc<Mutex<ReplayState>>) -> Self {
        Self { state }
    }

    fn lock(&self) -> MutexGuard<'_, ReplayState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn run_id(&self) -> RunId {
        self.lock().run_id
    }

    pub fn workflow_id(&self) -> WorkflowId {
        self.lock().workflow_id
    }

    /// Workflow time: the timestamp of the newest event replay has reached.
    /// Same value on every replay, unlike `Utc::now()`.
    pub fn now(&self) -> DateTime<Utc> {
        self.lock().now()
    }

    /// Run an activity with default options.
    pub async fn activity<I: Serialize, O: DeserializeOwned>(
        &self,
        name: &str,
        input: &I,
    ) -> Result<O> {
        self.activity_with(name, input, ActivityOptions::default())
            .await
    }

    pub async fn activity_with<I: Serialize, O: DeserializeOwned>(
        &self,
        name: &str,
        input: &I,
        options: ActivityOptions,
    ) -> Result<O> {
        let input = Payload::from_json(input)?;
        let out = ActivityStep {
            state: self.state.clone(),
            slot: None,
            request: Some((name.to_string(), input, options)),
        }
        .await?;
        Ok(out.as_json()?)
    }

    /// Durable sleep. Survives restarts because the timer is in history.
    pub async fn sleep(&self, duration: std::time::Duration) -> Result<()> {
        TimerStep {
            state: self.state.clone(),
            slot: None,
            duration: Some(duration),
        }
        .await
    }

    /// Wait for the next signal called `name` that this code hasn't consumed yet.
    pub async fn signal<T: DeserializeOwned>(&self, name: &str) -> Result<T> {
        let p = SignalStep {
            state: self.state.clone(),
            name: name.to_string(),
            ordinal: None,
        }
        .await;
        Ok(p.as_json()?)
    }

    /// Run `f` once and record its result. Replays return the recorded
    /// value without calling `f`. Use it for ids, random numbers, clock reads.
    pub fn side_effect<T, F>(&self, f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> T,
    {
        let mut st = self.lock();
        let i = st.next_marker;
        st.next_marker += 1;
        if let Some(p) = st.markers.get(i) {
            return Ok(p.as_json()?);
        }
        let value = f();
        let payload = Payload::from_json(&value)?;
        st.markers.push(payload.clone());
        st.decisions.push(DecisionKind::RecordMarker {
            name: "side_effect".into(),
            value: payload,
        });
        Ok(value)
    }
}

struct ActivityStep {
    state: Arc<Mutex<ReplayState>>,
    slot: Option<usize>,
    request: Option<(String, Payload, ActivityOptions)>,
}

impl Future for ActivityStep {
    type Output = Result<Payload>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = self.state.clone();
        let mut st = state.lock().unwrap_or_else(|e| e.into_inner());
        let idx = match self.slot {
            Some(i) => i,
            None => {
                let (ty, input, options) = self.request.take().expect("polled after completion");
                let i = st.next_activity;
                st.next_activity += 1;
                match st.activities.get(i) {
                    Some(slot) if slot.activity_type != ty => {
                        let msg = format!(
                            "activity #{i} is {:?} in history but code asked for {ty:?}",
                            slot.activity_type
                        );
                        return Poll::Ready(Err(st.fail(CascadeError::NonDeterminism(msg))));
                    }
                    Some(_) => {}
                    None => {
                        let id = ActivityId::new();
                        st.decisions.push(DecisionKind::ScheduleActivity {
                            activity_id: id,
                            activity_type: ty.clone(),
                            input: input.clone(),
                            options: options.clone(),
                        });
                        st.activities.push(ActivitySlot {
                            id,
                            activity_type: ty,
                            input,
                            options,
                            outcome: None,
                            failed_attempts: 0,
                        });
                    }
                }
                self.slot = Some(i);
                i
            }
        };
        match &st.activities[idx].outcome {
            Some((at, res)) if *at < st.visible => {
                Poll::Ready(res.clone().map_err(CascadeError::ActivityFailed))
            }
            _ => {
                st.wakers.push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

struct TimerStep {
    state: Arc<Mutex<ReplayState>>,
    slot: Option<usize>,
    duration: Option<std::time::Duration>,
}

impl Future for TimerStep {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = self.state.clone();
        let mut st = state.lock().unwrap_or_else(|e| e.into_inner());
        let idx = match self.slot {
            Some(i) => i,
            None => {
                let d = self.duration.take().expect("polled after completion");
                let i = st.next_timer;
                st.next_timer += 1;
                if i >= st.timers.len() {
                    // fire_at is recorded, so using the real clock here is fine.
                    let fire_at =
                        Utc::now() + chrono::Duration::from_std(d).unwrap_or(chrono::Duration::MAX);
                    let id = TimerId::new();
                    st.decisions.push(DecisionKind::StartTimer {
                        timer_id: id,
                        fire_at,
                    });
                    st.timers.push(TimerSlot {
                        id,
                        fire_at,
                        fired_at: None,
                    });
                }
                self.slot = Some(i);
                i
            }
        };
        match st.timers[idx].fired_at {
            Some(at) if at < st.visible => Poll::Ready(Ok(())),
            _ => {
                st.wakers.push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

struct SignalStep {
    state: Arc<Mutex<ReplayState>>,
    name: String,
    ordinal: Option<usize>,
}

impl Future for SignalStep {
    type Output = Payload;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = self.state.clone();
        let mut st = state.lock().unwrap_or_else(|e| e.into_inner());
        let k = match self.ordinal {
            Some(k) => k,
            None => {
                let c = st.signal_cursor.entry(self.name.clone()).or_insert(0);
                let k = *c;
                *c += 1;
                self.ordinal = Some(k);
                k
            }
        };
        let visible = st.visible;
        let found = st
            .signals
            .iter()
            .filter(|s| s.name == self.name && s.at < visible)
            .nth(k)
            .map(|s| s.input.clone());
        match found {
            Some(p) => Poll::Ready(p),
            None => {
                st.wakers.push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// Collects compensations as a workflow makes progress and runs them in
/// reverse if a later step fails.
///
/// ```ignore
/// let mut saga = Saga::new(&ctx);
/// ctx.activity::<_, ()>("reserve", &order).await?;
/// saga.add("release", &order)?;
/// if let Err(e) = ctx.activity::<_, ()>("charge", &order).await {
///     saga.compensate().await;
///     return Err(e);
/// }
/// ```
pub struct Saga {
    ctx: WorkflowContext,
    steps: Vec<(String, Payload)>,
}

impl Saga {
    pub fn new(ctx: &WorkflowContext) -> Self {
        Self {
            ctx: ctx.clone(),
            steps: Vec::new(),
        }
    }

    pub fn add<I: Serialize>(&mut self, activity: &str, input: &I) -> Result<()> {
        self.steps
            .push((activity.to_string(), Payload::from_json(input)?));
        Ok(())
    }

    /// Runs every compensation, newest first. Keeps going past failures and
    /// returns them so the caller can decide what to report.
    pub async fn compensate(self) -> Vec<CascadeError> {
        let mut errors = Vec::new();
        for (name, input) in self.steps.into_iter().rev() {
            let step = ActivityStep {
                state: self.ctx.state.clone(),
                slot: None,
                request: Some((name, input, ActivityOptions::default())),
            };
            if let Err(e) = step.await {
                errors.push(e);
            }
        }
        errors
    }
}
