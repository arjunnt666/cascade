//! JSON over HTTP in front of a `Cascade` host.
//!
//! POST /workflows/:type              body = input, returns {"run_id": ...}
//! GET  /runs/:id                     status, plus result/error once done
//! GET  /runs/:id/history             raw events
//! POST /runs/:id/signals/:name       body = signal input
//! POST /runs/:id/cancel              body = {"reason": "..."} (optional)
//!
//! No auth. Bind it to localhost or put something in front of it.

use std::net::SocketAddr;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use cascade_core::{CascadeError, EventPayload, Payload, RunId, WorkflowStatus};
use cascade_runtime::Cascade;
use serde::Deserialize;
use serde_json::{json, Value};

pub fn router(host: Cascade) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/workflows", get(list_workflows))
        .route("/workflows/:workflow_type", post(start))
        .route("/runs/:run_id", get(describe))
        .route("/runs/:run_id/history", get(history))
        .route("/runs/:run_id/signals/:name", post(signal))
        .route("/runs/:run_id/cancel", post(cancel))
        .with_state(host)
}

pub async fn serve(host: Cascade, addr: SocketAddr) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "cascade listening");
    axum::serve(listener, router(host)).await
}

struct ApiError(StatusCode, String);

impl From<CascadeError> for ApiError {
    fn from(e: CascadeError) -> Self {
        let code = match &e {
            CascadeError::RunNotFound(_) | CascadeError::UnknownWorkflow(_) => {
                StatusCode::NOT_FOUND
            }
            CascadeError::InvalidTransition(_) => StatusCode::CONFLICT,
            CascadeError::Serde(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        ApiError(code, e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

fn parse_run(s: &str) -> Result<RunId, ApiError> {
    s.parse()
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, format!("bad run id: {s}")))
}

/// Empty body means null, so `curl -X POST` with no data works.
fn body_payload(body: &axum::body::Bytes) -> Result<Payload, ApiError> {
    if body.is_empty() {
        return Ok(Payload::empty());
    }
    let v: Value = serde_json::from_slice(body)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("body is not json: {e}")))?;
    Ok(Payload::from_json(&v).expect("json value serializes"))
}

async fn list_workflows(State(host): State<Cascade>) -> Json<Vec<String>> {
    Json(
        host.workflow_types()
            .into_iter()
            .map(String::from)
            .collect(),
    )
}

async fn start(
    State(host): State<Cascade>,
    Path(workflow_type): Path<String>,
    body: axum::body::Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let run = host
        .start_payload(&workflow_type, body_payload(&body)?)
        .await?;
    Ok((StatusCode::CREATED, Json(json!({ "run_id": run }))))
}

async fn describe(State(host): State<Cascade>, Path(run_id): Path<String>) -> ApiResult<Value> {
    let run = parse_run(&run_id)?;
    let events = host.history(run).await?;
    if events.is_empty() {
        return Err(CascadeError::RunNotFound(run_id).into());
    }
    let status = host.status(run).await?;
    let mut out = json!({
        "run_id": run,
        "status": status,
        "events": events.len(),
    });
    if let EventPayload::WorkflowStarted { workflow_type, .. } = &events[0].payload {
        out["workflow_type"] = json!(workflow_type);
    }
    if status != WorkflowStatus::Running {
        match &events
            .iter()
            .rev()
            .find(|e| e.is_terminal())
            .map(|e| &e.payload)
        {
            Some(EventPayload::WorkflowCompleted { result }) => {
                out["result"] = result.as_json::<Value>().unwrap_or(Value::Null);
            }
            Some(EventPayload::WorkflowFailed { reason })
            | Some(EventPayload::WorkflowCancelled { reason }) => {
                out["error"] = json!(reason);
            }
            _ => {}
        }
    }
    Ok(Json(out))
}

async fn history(State(host): State<Cascade>, Path(run_id): Path<String>) -> ApiResult<Value> {
    let events = host.history(parse_run(&run_id)?).await?;
    if events.is_empty() {
        return Err(CascadeError::RunNotFound(run_id).into());
    }
    Ok(Json(
        serde_json::to_value(events).map_err(CascadeError::from)?,
    ))
}

async fn signal(
    State(host): State<Cascade>,
    Path((run_id, name)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Result<StatusCode, ApiError> {
    let input: Value = body_payload(&body)?.as_json().map_err(CascadeError::from)?;
    host.signal(parse_run(&run_id)?, &name, &input).await?;
    Ok(StatusCode::ACCEPTED)
}

#[derive(Deserialize, Default)]
struct CancelBody {
    reason: Option<String>,
}

async fn cancel(
    State(host): State<Cascade>,
    Path(run_id): Path<String>,
    body: axum::body::Bytes,
) -> Result<StatusCode, ApiError> {
    let body: CancelBody = if body.is_empty() {
        CancelBody::default()
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?
    };
    let reason = body.reason.unwrap_or_else(|| "cancelled over http".into());
    host.cancel(parse_run(&run_id)?, &reason).await?;
    Ok(StatusCode::ACCEPTED)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use cascade_runtime::WorkflowContext;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn app() -> Router {
        let host = Cascade::builder()
            .workflow("greet", |ctx: WorkflowContext, name: String| async move {
                let who: String = ctx.signal("confirm").await?;
                Ok(format!("hi {name}, from {who}"))
            })
            .build();
        router(host)
    }

    async fn call(app: &Router, method: &str, uri: &str, body: &str) -> (StatusCode, Value) {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn start_signal_finish() {
        let app = app();
        let (code, body) = call(&app, "POST", "/workflows/greet", "\"arjun\"").await;
        assert_eq!(code, StatusCode::CREATED);
        let run = body["run_id"].as_str().unwrap().to_string();

        let (_, body) = call(&app, "GET", &format!("/runs/{run}"), "").await;
        assert_eq!(body["status"], "Running");

        let (code, _) = call(
            &app,
            "POST",
            &format!("/runs/{run}/signals/confirm"),
            "\"server\"",
        )
        .await;
        assert_eq!(code, StatusCode::ACCEPTED);

        let (_, body) = call(&app, "GET", &format!("/runs/{run}"), "").await;
        assert_eq!(body["status"], "Completed");
        assert_eq!(body["result"], "hi arjun, from server");

        let (_, body) = call(&app, "GET", &format!("/runs/{run}/history"), "").await;
        assert_eq!(body.as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn errors_map_to_status_codes() {
        let app = app();
        assert_eq!(
            call(&app, "POST", "/workflows/nope", "").await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            call(&app, "GET", "/runs/not-a-uuid", "").await.0,
            StatusCode::BAD_REQUEST
        );
        let missing = RunId::new();
        assert_eq!(
            call(&app, "GET", &format!("/runs/{missing}"), "").await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            call(&app, "POST", "/workflows/greet", "{oops").await.0,
            StatusCode::BAD_REQUEST
        );

        let (_, body) = call(&app, "POST", "/workflows/greet", "\"x\"").await;
        let run = body["run_id"].as_str().unwrap().to_string();
        assert_eq!(
            call(&app, "POST", &format!("/runs/{run}/cancel"), "")
                .await
                .0,
            StatusCode::ACCEPTED
        );
        assert_eq!(
            call(&app, "POST", &format!("/runs/{run}/cancel"), "")
                .await
                .0,
            StatusCode::CONFLICT
        );
        let (_, body) = call(&app, "GET", &format!("/runs/{run}"), "").await;
        assert_eq!(body["status"], "Cancelled");
        assert_eq!(body["error"], "cancelled over http");
    }
}
