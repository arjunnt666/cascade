//! Low-level run API. You make the decisions yourself and feed results in.
//! `Cascade` (host.rs) is built on the same history and does this for you.

use cascade_core::{
    ActivityId, CascadeError, EventPayload, EventType, HistoryEvent, Payload, Result, RunId,
    TimerId, WorkflowId, WorkflowOptions, WorkflowStatus,
};
use cascade_history::HistoryStore;
use tracing::{debug, info};

use crate::decision::Decision;

pub struct RuntimeEngine {
    history: HistoryStore,
}

impl RuntimeEngine {
    pub fn new() -> Self {
        Self::with_history(HistoryStore::memory())
    }

    pub fn with_history(history: HistoryStore) -> Self {
        Self { history }
    }

    pub fn history(&self) -> &HistoryStore {
        &self.history
    }

    pub async fn start(
        &self,
        workflow_type: &str,
        input: Payload,
        options: WorkflowOptions,
    ) -> Result<(WorkflowId, RunId)> {
        let (workflow_id, run_id) = (WorkflowId::new(), RunId::new());
        let started = HistoryEvent::new(
            run_id,
            workflow_id,
            0,
            EventPayload::WorkflowStarted {
                workflow_type: workflow_type.to_string(),
                input,
                task_queue: options.task_queue,
            },
        );
        self.history.append(&run_id, vec![started]).await?;
        info!(%workflow_id, %run_id, workflow_type, "workflow started");
        Ok((workflow_id, run_id))
    }

    pub async fn events(&self, run_id: &RunId) -> Result<Vec<HistoryEvent>> {
        self.history.load(run_id, 0).await
    }

    /// Append payloads after whatever is in history now. Refuses to write
    /// past a terminal event.
    pub async fn append(&self, run_id: &RunId, payloads: Vec<EventPayload>) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }
        let events = self.events(run_id).await?;
        let first = events
            .first()
            .ok_or_else(|| CascadeError::RunNotFound(run_id.to_string()))?;
        if events.iter().any(HistoryEvent::is_terminal) {
            return Err(CascadeError::InvalidTransition(format!(
                "run {run_id} already finished"
            )));
        }
        let wf = first.workflow_id;
        let base = events.len() as u64;
        let new: Vec<_> = payloads
            .into_iter()
            .enumerate()
            .map(|(i, p)| HistoryEvent::new(*run_id, wf, base + i as u64, p))
            .collect();
        debug!(%run_id, count = new.len(), "appending events");
        self.history.append(run_id, new).await?;
        Ok(())
    }

    pub async fn apply_decisions(&self, run_id: &RunId, decisions: Vec<Decision>) -> Result<()> {
        let payloads = decisions
            .into_iter()
            .map(|d| d.kind.into_payload())
            .collect();
        self.append(run_id, payloads).await
    }

    pub async fn complete_activity(
        &self,
        run_id: &RunId,
        activity_id: ActivityId,
        result: Payload,
    ) -> Result<()> {
        self.append(
            run_id,
            vec![EventPayload::ActivityCompleted {
                activity_id,
                result,
            }],
        )
        .await
    }

    pub async fn fail_activity(
        &self,
        run_id: &RunId,
        activity_id: ActivityId,
        reason: String,
        attempt: u32,
        will_retry: bool,
    ) -> Result<()> {
        self.append(
            run_id,
            vec![EventPayload::ActivityFailed {
                activity_id,
                reason,
                attempt,
                will_retry,
            }],
        )
        .await
    }

    pub async fn fire_timer(&self, run_id: &RunId, timer_id: TimerId) -> Result<()> {
        self.append(run_id, vec![EventPayload::TimerFired { timer_id }])
            .await
    }

    pub async fn get_status(&self, run_id: &RunId) -> Result<WorkflowStatus> {
        let events = self.events(run_id).await?;
        if events.is_empty() {
            return Err(CascadeError::RunNotFound(run_id.to_string()));
        }
        Ok(status_of(&events))
    }
}

impl Default for RuntimeEngine {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn status_of(events: &[HistoryEvent]) -> WorkflowStatus {
    match events
        .iter()
        .rev()
        .find(|e| e.is_terminal())
        .map(|e| e.event_type)
    {
        Some(EventType::WorkflowCompleted) => WorkflowStatus::Completed,
        Some(EventType::WorkflowFailed) => WorkflowStatus::Failed,
        Some(EventType::WorkflowCancelled) => WorkflowStatus::Cancelled,
        Some(EventType::WorkflowContinuedAsNew) => WorkflowStatus::ContinuedAsNew,
        _ => WorkflowStatus::Running,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DecisionKind;
    use cascade_core::ActivityOptions;
    use serde_json::json;

    fn kinds(events: &[HistoryEvent]) -> Vec<EventType> {
        events.iter().map(|e| e.event_type).collect()
    }

    #[tokio::test]
    async fn schedule_complete_finish() {
        let engine = RuntimeEngine::new();
        let (_wf, run) = engine
            .start(
                "order",
                Payload::from_json(&json!({"sku": "x"})).unwrap(),
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
        let act = ActivityId::new();
        engine
            .apply_decisions(
                &run,
                vec![DecisionKind::ScheduleActivity {
                    activity_id: act,
                    activity_type: "charge".into(),
                    input: Payload::empty(),
                    options: ActivityOptions::default(),
                }
                .into()],
            )
            .await
            .unwrap();
        engine
            .complete_activity(&run, act, Payload::from_json(&json!({"ok": true})).unwrap())
            .await
            .unwrap();
        engine
            .apply_decisions(
                &run,
                vec![DecisionKind::CompleteWorkflow {
                    result: Payload::from_json(&json!({"charged": true})).unwrap(),
                }
                .into()],
            )
            .await
            .unwrap();
        assert_eq!(
            engine.get_status(&run).await.unwrap(),
            WorkflowStatus::Completed
        );
        assert_eq!(
            kinds(&engine.events(&run).await.unwrap()),
            [
                EventType::WorkflowStarted,
                EventType::ActivityScheduled,
                EventType::ActivityCompleted,
                EventType::WorkflowCompleted,
            ]
        );
    }

    #[tokio::test]
    async fn cancel_timer_records_cancel_not_fire() {
        let engine = RuntimeEngine::new();
        let (_wf, run) = engine
            .start("wait", Payload::empty(), WorkflowOptions::default())
            .await
            .unwrap();
        let timer = TimerId::new();
        engine
            .apply_decisions(
                &run,
                vec![
                    DecisionKind::StartTimer {
                        timer_id: timer,
                        fire_at: chrono::Utc::now(),
                    }
                    .into(),
                    DecisionKind::CancelTimer { timer_id: timer }.into(),
                ],
            )
            .await
            .unwrap();
        let events = engine.events(&run).await.unwrap();
        assert_eq!(
            kinds(&events),
            [
                EventType::WorkflowStarted,
                EventType::TimerStarted,
                EventType::TimerCancelled
            ]
        );
        assert!(matches!(
            events[2].payload,
            EventPayload::TimerCancelled { .. }
        ));
        assert_eq!(
            engine.get_status(&run).await.unwrap(),
            WorkflowStatus::Running
        );
    }

    #[tokio::test]
    async fn nothing_lands_after_terminal_event() {
        let engine = RuntimeEngine::new();
        let (_wf, run) = engine
            .start("x", Payload::empty(), WorkflowOptions::default())
            .await
            .unwrap();
        engine
            .apply_decisions(
                &run,
                vec![DecisionKind::ContinueAsNew {
                    input: Payload::empty(),
                }
                .into()],
            )
            .await
            .unwrap();
        assert_eq!(
            engine.get_status(&run).await.unwrap(),
            WorkflowStatus::ContinuedAsNew
        );
        assert!(engine.fire_timer(&run, TimerId::new()).await.is_err());
    }

    #[tokio::test]
    async fn unknown_run() {
        let engine = RuntimeEngine::new();
        assert!(matches!(
            engine.get_status(&RunId::new()).await,
            Err(CascadeError::RunNotFound(_))
        ));
    }
}
