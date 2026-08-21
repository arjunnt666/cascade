use std::sync::Arc;

use cascade_core::{
    ActivityId, EventPayload, EventType, HistoryEvent, Payload, Result, RunId, WorkflowId,
    WorkflowOptions, WorkflowStatus,
};
use cascade_history::{HistoryStore, MemoryHistoryStore};
use tracing::{debug, info};

use crate::decision::{Decision, DecisionKind};

/// Simple single-process runtime. History is the source of truth.
pub struct RuntimeEngine {
    history: Arc<HistoryStore<MemoryHistoryStore>>,
}

impl RuntimeEngine {
    pub fn new() -> Self {
        Self {
            history: Arc::new(HistoryStore::new(MemoryHistoryStore::new())),
        }
    }

    pub fn with_history(history: HistoryStore<MemoryHistoryStore>) -> Self {
        Self {
            history: Arc::new(history),
        }
    }

    pub async fn start(
        &self,
        workflow_type: &str,
        input: Payload,
        options: WorkflowOptions,
    ) -> Result<(WorkflowId, RunId)> {
        let workflow_id = WorkflowId::new();
        let run_id = RunId::new();

        let start_event = HistoryEvent {
            id: cascade_core::EventId::new(),
            run_id,
            workflow_id,
            sequence: 0,
            event_type: EventType::WorkflowStarted,
            payload: EventPayload::WorkflowStarted {
                workflow_type: workflow_type.to_string(),
                input,
                task_queue: options.task_queue.clone(),
            },
            timestamp: chrono::Utc::now(),
        };

        self.history.append(&run_id, vec![start_event]).await?;
        info!(%workflow_id, %run_id, workflow_type, "workflow started");
        Ok((workflow_id, run_id))
    }

    pub async fn events(&self, run_id: &RunId) -> Result<Vec<HistoryEvent>> {
        self.history.load(run_id, 0).await
    }

    async fn next_seq(&self, run_id: &RunId) -> Result<(WorkflowId, u64)> {
        let events = self.history.load(run_id, 0).await?;
        let wf = events
            .first()
            .map(|e| e.workflow_id)
            .ok_or_else(|| cascade_core::CascadeError::RunNotFound(run_id.to_string()))?;
        Ok((wf, events.len() as u64))
    }

    pub async fn apply_decisions(&self, run_id: &RunId, decisions: Vec<Decision>) -> Result<()> {
        if decisions.is_empty() {
            return Ok(());
        }
        let (workflow_id, mut seq) = self.next_seq(run_id).await?;
        let mut events = Vec::with_capacity(decisions.len());
        for d in decisions {
            let (event_type, payload) = match d.kind {
                DecisionKind::ScheduleActivity {
                    activity_id,
                    activity_type,
                    input,
                    options,
                } => (
                    EventType::ActivityScheduled,
                    EventPayload::ActivityScheduled {
                        activity_id,
                        activity_type,
                        input,
                        options,
                    },
                ),
                DecisionKind::CompleteWorkflow { result } => (
                    EventType::WorkflowCompleted,
                    EventPayload::WorkflowCompleted { result },
                ),
                DecisionKind::FailWorkflow { reason } => (
                    EventType::WorkflowFailed,
                    EventPayload::WorkflowFailed { reason },
                ),
                DecisionKind::StartTimer { timer_id, fire_at } => (
                    EventType::TimerStarted,
                    EventPayload::TimerStarted { timer_id, fire_at },
                ),
                DecisionKind::CancelTimer { timer_id } => (
                    EventType::TimerCancelled,
                    EventPayload::TimerFired { timer_id },
                ),
                DecisionKind::ContinueAsNew { input } => (
                    EventType::MarkerRecorded,
                    EventPayload::SignalReceived {
                        signal_name: "continue_as_new".into(),
                        input,
                    },
                ),
            };
            events.push(HistoryEvent {
                id: cascade_core::EventId::new(),
                run_id: *run_id,
                workflow_id,
                sequence: seq,
                event_type,
                payload,
                timestamp: chrono::Utc::now(),
            });
            seq += 1;
        }
        debug!(%run_id, count = events.len(), "applying decisions");
        self.history.append(run_id, events).await?;
        Ok(())
    }

    pub async fn complete_activity(
        &self,
        run_id: &RunId,
        activity_id: ActivityId,
        result: Payload,
    ) -> Result<()> {
        let (workflow_id, seq) = self.next_seq(run_id).await?;
        let event = HistoryEvent {
            id: cascade_core::EventId::new(),
            run_id: *run_id,
            workflow_id,
            sequence: seq,
            event_type: EventType::ActivityCompleted,
            payload: EventPayload::ActivityCompleted {
                activity_id,
                result,
            },
            timestamp: chrono::Utc::now(),
        };
        self.history.append(run_id, vec![event]).await?;
        Ok(())
    }

    pub async fn get_status(&self, run_id: &RunId) -> Result<WorkflowStatus> {
        let events = self.history.load(run_id, 0).await?;
        if let Some(last) = events.last() {
            match last.event_type {
                EventType::WorkflowCompleted => Ok(WorkflowStatus::Completed),
                EventType::WorkflowFailed => Ok(WorkflowStatus::Failed),
                EventType::WorkflowCancelled => Ok(WorkflowStatus::Cancelled),
                _ => Ok(WorkflowStatus::Running),
            }
        } else {
            Ok(WorkflowStatus::Running)
        }
    }
}

impl Default for RuntimeEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cascade_core::ActivityOptions;

    #[tokio::test]
    async fn schedule_complete_finish() {
        let engine = RuntimeEngine::new();
        let (_wf, run) = engine
            .start("order", Payload::from_json(&serde_json::json!({"sku":"x"})).unwrap(), WorkflowOptions::default())
            .await
            .unwrap();
        let act = ActivityId::new();
        engine
            .apply_decisions(
                &run,
                vec![Decision {
                    kind: DecisionKind::ScheduleActivity {
                        activity_id: act,
                        activity_type: "charge".into(),
                        input: Payload::empty(),
                        options: ActivityOptions::default(),
                    },
                }],
            )
            .await
            .unwrap();
        engine
            .complete_activity(&run, act, Payload::from_json(&serde_json::json!({"ok":true})).unwrap())
            .await
            .unwrap();
        engine
            .apply_decisions(
                &run,
                vec![Decision {
                    kind: DecisionKind::CompleteWorkflow {
                        result: Payload::from_json(&serde_json::json!({"charged":true})).unwrap(),
                    },
                }],
            )
            .await
            .unwrap();
        assert_eq!(engine.get_status(&run).await.unwrap(), WorkflowStatus::Completed);
        let events = engine.events(&run).await.unwrap();
        let kinds: Vec<_> = events.iter().map(|e| e.event_type.clone()).collect();
        assert_eq!(
            kinds,
            vec![
                EventType::WorkflowStarted,
                EventType::ActivityScheduled,
                EventType::ActivityCompleted,
                EventType::WorkflowCompleted,
            ]
        );
    }
}
