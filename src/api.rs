//! HTTP API built with axum.
//!
//! Endpoints
//! ---------
//! - `GET    /healthz`                       liveness probe
//! - `GET    /metrics`                       Prometheus text metrics
//! - `GET    /v1/status`                     runtime status
//! - `GET    /v1/kv/{key}`                   read one key
//! - `PUT    /v1/kv/{key}`                   write one key
//! - `DELETE /v1/kv/{key}`                   delete one key
//! - `PUT    /v1/kv/{key}/cas`               compare-and-swap write
//! - `POST   /v1/txn`                        atomic multi-key transaction
//! - `GET    /v1/kv?prefix=...`              prefix scan
//! - `GET    /v1/kv?start=...&end=...`       range scan
//! - `POST   /v1/compact?wait=true|false`    trigger snapshot compaction
//!
//! All errors use one structured JSON body: `{"error": {"kind", ...}}`.

use std::ops::Bound;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, Query, Request, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::error::StoreError;
use crate::store::{Op, OpEffect, Store};

/// Maximum accepted HTTP request body (32 MiB).
const MAX_BODY: usize = 32 * 1024 * 1024;
/// Default and maximum number of entries returned by one scan.
const DEFAULT_SCAN_LIMIT: usize = 1000;
const MAX_SCAN_LIMIT: usize = 100_000;

/// Application state shared by handlers.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
}

// ---- structured errors ----------------------------------------------

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub error: ErrorDetail,
}

#[derive(Debug, Serialize)]
pub struct ErrorDetail {
    pub kind: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<ValueJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<ValueJson>,
}

/// JSON representation of an optional binary value (null or string).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ValueJson {
    Null,
    String(String),
}

/// Deserialize a field that distinguishes "field absent" (`None`) from
/// "field present and JSON null" (`Some(ValueJson::Null)`); plain
/// `Option<T>` cannot tell the two apart, but CAS needs null to mean
/// "the key must be absent" explicitly.
fn present_or_absent<'de, D>(de: D) -> Result<Option<ValueJson>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Some(ValueJson::deserialize(de)?))
}

impl ValueJson {
    fn of(v: Option<&Vec<u8>>) -> Option<ValueJson> {
        Some(match v {
            None => ValueJson::Null,
            Some(b) => ValueJson::String(render_value(b)),
        })
    }
}

/// Error type used by handlers.
#[derive(Debug)]
pub enum AppError {
    BadRequest(String),
    Store(StoreError),
}

impl From<StoreError> for AppError {
    fn from(e: StoreError) -> Self {
        AppError::Store(e)
    }
}

impl AppError {
    fn status(&self) -> StatusCode {
        match self {
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Store(e) => match e {
                StoreError::CasConflict { .. } | StoreError::CompactionInProgress => {
                    StatusCode::CONFLICT
                }
                StoreError::EmptyKey
                | StoreError::KeyTooLarge { .. }
                | StoreError::ValueTooLarge { .. }
                | StoreError::EmptyTransaction
                | StoreError::TransactionTooLarge { .. }
                | StoreError::DuplicateKeyInTransaction { .. } => StatusCode::BAD_REQUEST,
                StoreError::Corruption(_) | StoreError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();
        let detail = match &self {
            AppError::BadRequest(msg) => ErrorDetail {
                kind: "invalid_request".to_string(),
                message: msg.clone(),
                key: None,
                expected: None,
                actual: None,
            },
            AppError::Store(StoreError::CasConflict {
                key,
                expected,
                actual,
            }) => ErrorDetail {
                kind: "cas_conflict".to_string(),
                message: format!(
                    "compare-and-swap conflict on key '{}': current value does not match expected",
                    String::from_utf8_lossy(key)
                ),
                key: Some(String::from_utf8_lossy(key).into_owned()),
                expected: ValueJson::of(expected.as_ref()),
                actual: ValueJson::of(actual.as_ref()),
            },
            AppError::Store(e) => ErrorDetail {
                kind: e.kind().to_string(),
                message: e.to_string(),
                key: None,
                expected: None,
                actual: None,
            },
        };
        if status.is_server_error() {
            tracing::error!(?status, error = %detail.message, "request failed");
        }
        (status, Json(ErrorBody { error: detail })).into_response()
    }
}

/// Middleware that counts every served request by status class. It wraps
/// the whole router (including the fallback), so handlers and the error
/// conversion need no metric bookkeeping.
async fn metrics_layer(
    State(state): State<AppState>,
    req: Request,
    next: axum::middleware::Next,
) -> Response {
    let metrics = state.store.metrics.clone();
    let resp = next.run(req).await;
    metrics.record_status(resp.status().as_u16());
    resp
}

// ---- response DTOs ---------------------------------------------------

#[derive(Debug, Serialize)]
struct ValuePayload {
    key: String,
    found: bool,
    value: ValueJson,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u64>,
    read_lsn: u64,
}

#[derive(Debug, Serialize)]
struct EntryPayload {
    key: String,
    value: ValueJson,
    version: u64,
}

#[derive(Debug, Serialize)]
struct ScanResponse {
    read_lsn: u64,
    count: usize,
    entries: Vec<EntryPayload>,
}

#[derive(Debug, Serialize)]
struct CommitResponse {
    lsn: u64,
    version: u64,
    /// For put: value replaced an existing key; for delete: key existed;
    /// for cas: an existing value was overwritten.
    changed: bool,
}

#[derive(Debug, Serialize)]
struct EffectPayload {
    op: &'static str,
    key: String,
    version: Option<u64>,
    /// Whether the operation changed existing state.
    changed: bool,
    /// Whether the key already held a value (put/cas only).
    replaced: bool,
}

#[derive(Debug, Serialize)]
struct TxnResponse {
    lsn: u64,
    effects: Vec<EffectPayload>,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    key_count: usize,
    lsn: u64,
    snapshot_lsn: u64,
    wal_total_bytes: u64,
    wal_active_segment: u64,
    compacting: bool,
    limits: LimitsPayload,
}

#[derive(Debug, Serialize)]
struct LimitsPayload {
    max_key_bytes: usize,
    max_value_bytes: usize,
    max_ops_per_txn: usize,
}

#[derive(Debug, Serialize)]
struct CompactResponse {
    started: bool,
    snapshot_lsn: Option<u64>,
}

// ---- request DTOs ----------------------------------------------------

#[derive(Debug, Deserialize)]
struct ValueBody {
    value: Option<String>,
    value_base64: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CasBody {
    /// Required and explicit: null means the key must be absent.
    #[serde(default, deserialize_with = "present_or_absent")]
    expected: Option<ValueJson>,
    value: Option<String>,
    value_base64: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpBody {
    op: String,
    key: String,
    value: Option<String>,
    value_base64: Option<String>,
    #[serde(default, deserialize_with = "present_or_absent")]
    expected: Option<ValueJson>,
}

#[derive(Debug, Deserialize)]
struct TxnBody {
    ops: Vec<OpBody>,
}

#[derive(Debug, Deserialize)]
struct ScanParams {
    prefix: Option<String>,
    start: Option<String>,
    end: Option<String>,
    #[serde(default)]
    end_inclusive: bool,
    #[serde(default)]
    start_exclusive: bool,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct CompactParams {
    #[serde(default)]
    wait: bool,
}

// ---- helpers ---------------------------------------------------------

/// Render bytes for JSON output: valid UTF-8 as text, else base64 with a
/// `base64:` prefix.
fn render_value(v: &[u8]) -> String {
    match std::str::from_utf8(v) {
        Ok(s) => s.to_string(),
        Err(_) => format!(
            "base64:{}",
            base64::engine::general_purpose::STANDARD.encode(v)
        ),
    }
}

fn value_json(v: &[u8]) -> ValueJson {
    ValueJson::String(render_value(v))
}

fn decode_value(v: Option<String>, b64: Option<String>) -> Result<Vec<u8>, AppError> {
    match (v, b64) {
        (Some(s), None) => Ok(s.into_bytes()),
        (None, Some(s)) => base64::engine::general_purpose::STANDARD
            .decode(s.trim())
            .map_err(|e| AppError::BadRequest(format!("invalid base64 value: {e}"))),
        (None, None) => Err(AppError::BadRequest(
            "body must contain exactly one of 'value' or 'value_base64'".to_string(),
        )),
        (Some(_), Some(_)) => Err(AppError::BadRequest(
            "'value' and 'value_base64' are mutually exclusive".to_string(),
        )),
    }
}

fn decode_expected(v: Option<ValueJson>) -> Option<Vec<u8>> {
    match v {
        None | Some(ValueJson::Null) => None,
        Some(ValueJson::String(s)) => match s.strip_prefix("base64:") {
            Some(rest) => match base64::engine::general_purpose::STANDARD.decode(rest) {
                Ok(decoded) => Some(decoded),
                Err(_) => Some(s.into_bytes()),
            },
            None => Some(s.into_bytes()),
        },
    }
}

async fn read_json<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, AppError> {
    if body.is_empty() {
        return Err(AppError::BadRequest(
            "request body must not be empty".to_string(),
        ));
    }
    serde_json::from_slice(body)
        .map_err(|e| AppError::BadRequest(format!("invalid JSON body: {e}")))
}

fn ok<T: Serialize>(_state: &AppState, body: T) -> Response {
    Json(body).into_response()
}

// ---- handlers --------------------------------------------------------

async fn healthz() -> &'static str {
    "ok"
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    let s = state.store.status();
    let m = &state.store.metrics;
    m.keys.store(s.key_count as u64, Ordering::Relaxed);
    m.lsn.store(s.lsn, Ordering::Relaxed);
    m.wal_total_bytes
        .store(s.wal_total_bytes, Ordering::Relaxed);
    m.compacting.store(s.compacting as u64, Ordering::Relaxed);
    m.last_snapshot_lsn.store(s.snapshot_lsn, Ordering::Relaxed);
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        m.render_prometheus(),
    )
        .into_response()
}

async fn status_handler(State(state): State<AppState>) -> Response {
    let s = state.store.status();
    ok(
        &state,
        StatusResponse {
            key_count: s.key_count,
            lsn: s.lsn,
            snapshot_lsn: s.snapshot_lsn,
            wal_total_bytes: s.wal_total_bytes,
            wal_active_segment: s.wal_active_segment,
            compacting: s.compacting,
            limits: LimitsPayload {
                max_key_bytes: s.limits.max_key_bytes,
                max_value_bytes: s.limits.max_value_bytes,
                max_ops_per_txn: s.limits.max_ops_per_txn,
            },
        },
    )
}

async fn get_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Response, AppError> {
    let (view, read_lsn) = state.store.get(key.as_bytes()).await?;
    let payload = match view {
        Some(v) => ValuePayload {
            key: key.clone(),
            found: true,
            value: value_json(&v.value),
            version: Some(v.version),
            read_lsn,
        },
        None => ValuePayload {
            key: key.clone(),
            found: false,
            value: ValueJson::Null,
            version: None,
            read_lsn,
        },
    };
    let mut resp = ok(&state, payload);
    resp.headers_mut().insert(
        "X-Read-LSN",
        HeaderValue::from_str(&read_lsn.to_string()).unwrap(),
    );
    Ok(resp)
}

async fn put_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
    body: Bytes,
) -> Result<Response, AppError> {
    let body: ValueBody = read_json(&body).await?;
    let value = decode_value(body.value, body.value_base64)?;
    let out = state.store.put(key.into_bytes(), value).await?;
    match out.effects.first() {
        Some(OpEffect::Put {
            version, replaced, ..
        }) => Ok(ok(
            &state,
            CommitResponse {
                lsn: out.lsn,
                version: *version,
                changed: *replaced,
            },
        )),
        _ => unreachable!("put produces exactly one put effect"),
    }
}

async fn delete_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Response, AppError> {
    let out = state.store.delete(key.into_bytes()).await?;
    let existed = match out.effects.first() {
        Some(OpEffect::Delete { existed, .. }) => *existed,
        _ => unreachable!("delete produces exactly one delete effect"),
    };
    Ok(ok(
        &state,
        CommitResponse {
            lsn: out.lsn,
            version: out.lsn,
            changed: existed,
        },
    ))
}

async fn cas_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
    body: Bytes,
) -> Result<Response, AppError> {
    let body: CasBody = read_json(&body).await?;
    if body.expected.is_none() {
        return Err(AppError::BadRequest(
            "CAS body must contain 'expected' (string or null)".to_string(),
        ));
    }
    let expected = decode_expected(body.expected);
    let value = decode_value(body.value, body.value_base64)?;
    let out = state.store.cas(key.into_bytes(), expected, value).await?;
    match out.effects.first() {
        Some(OpEffect::Cas {
            version, replaced, ..
        }) => Ok(ok(
            &state,
            CommitResponse {
                lsn: out.lsn,
                version: *version,
                changed: *replaced,
            },
        )),
        _ => unreachable!("cas produces exactly one cas effect"),
    }
}

async fn txn_handler(State(state): State<AppState>, body: Bytes) -> Result<Response, AppError> {
    let body: TxnBody = read_json(&body).await?;

    let mut ops = Vec::with_capacity(body.ops.len());
    for ob in body.ops {
        let key = ob.key.into_bytes();
        let op = match ob.op.as_str() {
            "put" => {
                let value = decode_value(ob.value, ob.value_base64)?;
                Op::Put { key, value }
            }
            "delete" => {
                if ob.value.is_some() || ob.value_base64.is_some() {
                    return Err(AppError::BadRequest(
                        "delete operations must not carry a value".to_string(),
                    ));
                }
                Op::Delete { key }
            }
            "cas" => {
                if ob.expected.is_none() {
                    return Err(AppError::BadRequest(
                        "cas operation requires 'expected' (string or null)".to_string(),
                    ));
                }
                let expected = decode_expected(ob.expected);
                let value = decode_value(ob.value, ob.value_base64)?;
                Op::Cas {
                    key,
                    expected,
                    value,
                }
            }
            other => {
                return Err(AppError::BadRequest(format!(
                    "unknown op {other:?}; expected put, delete or cas"
                )))
            }
        };
        ops.push(op);
    }

    let out = state.store.commit(ops).await?;
    let effects = out
        .effects
        .iter()
        .map(|e| match e {
            OpEffect::Put {
                key,
                version,
                replaced,
            } => EffectPayload {
                op: "put",
                key: String::from_utf8_lossy(key).into_owned(),
                version: Some(*version),
                changed: true,
                replaced: *replaced,
            },
            OpEffect::Delete { key, existed } => EffectPayload {
                op: "delete",
                key: String::from_utf8_lossy(key).into_owned(),
                version: None,
                changed: *existed,
                replaced: false,
            },
            OpEffect::Cas {
                key,
                version,
                replaced,
            } => EffectPayload {
                op: "cas",
                key: String::from_utf8_lossy(key).into_owned(),
                version: Some(*version),
                changed: true,
                replaced: *replaced,
            },
        })
        .collect();
    Ok(ok(
        &state,
        TxnResponse {
            lsn: out.lsn,
            effects,
        },
    ))
}

async fn scan_handler(
    State(state): State<AppState>,
    Query(params): Query<ScanParams>,
) -> Result<Response, AppError> {
    if let Some(lim) = params.limit {
        if lim == 0 {
            return Err(AppError::BadRequest("limit must be >= 1".to_string()));
        }
    }
    let limit = params
        .limit
        .unwrap_or(DEFAULT_SCAN_LIMIT)
        .min(MAX_SCAN_LIMIT);

    let (entries, read_lsn) = match (&params.prefix, &params.start) {
        (Some(_), Some(_)) => {
            return Err(AppError::BadRequest(
                "'prefix' and 'start' are mutually exclusive".to_string(),
            ));
        }
        (Some(prefix), None) => state.store.scan_prefix(prefix.as_bytes(), limit).await?,
        (None, Some(start)) => {
            let start_bound = if params.start_exclusive {
                Bound::Excluded(start.as_bytes().to_vec())
            } else {
                Bound::Included(start.as_bytes().to_vec())
            };
            let end_bound = match &params.end {
                Some(e) if params.end_inclusive => Bound::Included(e.as_bytes().to_vec()),
                Some(e) => Bound::Excluded(e.as_bytes().to_vec()),
                None => Bound::Unbounded,
            };
            state
                .store
                .scan_range(start_bound, end_bound, limit)
                .await?
        }
        (None, None) => {
            return Err(AppError::BadRequest(
                "scan requires 'prefix' or 'start'".to_string(),
            ));
        }
    };

    let entries: Vec<EntryPayload> = entries
        .into_iter()
        .map(|(k, v)| EntryPayload {
            key: String::from_utf8_lossy(&k).into_owned(),
            value: value_json(&v.value),
            version: v.version,
        })
        .collect();
    let payload = ScanResponse {
        read_lsn,
        count: entries.len(),
        entries,
    };
    let mut resp = ok(&state, payload);
    resp.headers_mut().insert(
        "X-Read-LSN",
        HeaderValue::from_str(&read_lsn.to_string()).unwrap(),
    );
    Ok(resp)
}

async fn compact_handler(
    State(state): State<AppState>,
    Query(params): Query<CompactParams>,
) -> Result<Response, AppError> {
    let snapshot_lsn = state.store.compact(params.wait).await?;
    Ok(ok(
        &state,
        CompactResponse {
            started: true,
            snapshot_lsn,
        },
    ))
}

async fn fallback() -> Response {
    let body = ErrorBody {
        error: ErrorDetail {
            kind: "not_found".to_string(),
            message: "no such endpoint".to_string(),
            key: None,
            expected: None,
            actual: None,
        },
    };
    (StatusCode::NOT_FOUND, Json(body)).into_response()
}

/// Build the application router.
pub fn app(store: Arc<Store>) -> Router {
    let state = AppState {
        store: store.clone(),
    };
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_handler))
        .route("/v1/status", get(status_handler))
        .route("/v1/kv", get(scan_handler))
        .route("/v1/txn", post(txn_handler))
        .route("/v1/compact", post(compact_handler))
        .route(
            "/v1/kv/:key",
            get(get_key).put(put_handler).delete(delete_handler),
        )
        .route("/v1/kv/:key/cas", put(cas_handler))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            metrics_layer,
        ))
        .layer(axum::extract::DefaultBodyLimit::max(MAX_BODY))
        .fallback(fallback)
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_text_and_binary() {
        assert_eq!(render_value(b"hello"), "hello");
        assert!(render_value(&[0xff, 0xfe]).starts_with("base64:"));
    }
}
