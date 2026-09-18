//! HTTP API (axum).
//!
//! Keys and values may be supplied either as UTF-8 strings (`"key"`) or as
//! base64 in an object form (`{"base64": "..."}`). Responses always include
//! both the UTF-8 string (when representable) and the base64 form, plus the
//! observed log position (`read_lsn`).

use crate::error::{KvError, Result};
use crate::store::{Got, KvEntry, Status, Store};
use crate::wal::WalOp;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::Request;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub metrics: Arc<Metrics>,
}

#[derive(Debug)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub inflight: AtomicU64,
    pub errors_5xx_total: AtomicU64,
    pub requests_by_route: tokio::sync::Mutex<BTreeMap<String, u64>>,
    pub started_at: Instant,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            requests_total: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
            errors_5xx_total: AtomicU64::new(0),
            requests_by_route: tokio::sync::Mutex::new(BTreeMap::new()),
            started_at: Instant::now(),
        }
    }
}

pub fn router(store: Store) -> Router {
    let state = AppState {
        store,
        metrics: Arc::new(Metrics::new()),
    };
    Router::new()
        .route("/health", get(health))
        .route("/v1/status", get(status))
        .route("/v1/metrics", get(metrics_handler))
        .route("/v1/kv/{key}", get(get_key).put(put_key).delete(delete_key))
        .route("/v1/kv/{key}/cas", post(cas_key))
        .route("/v1/txn", post(txn))
        .route("/v1/range", post(range_query))
        .route("/v1/prefix/{prefix}", get(prefix_query))
        .route("/v1/admin/compact", post(manual_compact))
        .fallback(fallback)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            metrics_middleware,
        ))
        .with_state(state)
}

// ---------- JSON extraction with structured errors ----------

/// JSON body extractor that maps all parsing failures to `KvError::InvalidRequest`.
#[derive(Debug)]
pub struct ApiJson<T>(pub T);

impl<T, S> axum::extract::FromRequest<S> for ApiJson<T>
where
    T: for<'de> Deserialize<'de>,
    S: Send + Sync,
{
    type Rejection = KvError;

    async fn from_request(req: Request<axum::body::Body>, state: &S) -> Result<Self> {
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|e| KvError::InvalidRequest(format!("failed reading body: {e}")))?;
        if bytes.is_empty() {
            return Err(KvError::InvalidRequest("request body is empty".into()));
        }
        serde_json::from_slice::<T>(&bytes)
            .map(ApiJson)
            .map_err(|e| KvError::InvalidRequest(format!("invalid JSON: {e}")))
    }
}

// ---------- key/value codecs ----------

/// A field that can be either a JSON string or `{"base64": "..."}`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum BytesField {
    Str(String),
    B64 {
        #[serde(rename = "base64")]
        base64: String,
    },
}

impl BytesField {
    pub fn into_bytes(self, field: &str) -> Result<Vec<u8>> {
        match self {
            BytesField::Str(s) => Ok(s.into_bytes()),
            BytesField::B64 { base64 } => decode_b64(&base64, field),
        }
    }
}

fn decode_b64(s: &str, field: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| KvError::InvalidBase64 {
            field: field.into(),
            message: e.to_string(),
        })
}

/// URL path segment is already percent-decoded; treat its bytes as the key.
fn path_key(raw: &str) -> Vec<u8> {
    raw.as_bytes().to_vec()
}

#[derive(serde::Serialize)]
struct BytesOut {
    /// UTF-8 representation when the bytes are valid UTF-8.
    text: Option<String>,
    base64: String,
    bytes_len: usize,
}

fn bytes_out(b: &[u8]) -> BytesOut {
    BytesOut {
        text: std::str::from_utf8(b).ok().map(|s| s.to_string()),
        base64: base64::engine::general_purpose::STANDARD.encode(b),
        bytes_len: b.len(),
    }
}

// ---------- DTOs ----------

#[derive(Deserialize)]
struct PutReq {
    value: BytesField,
}

#[derive(Deserialize)]
struct CasReq {
    expected: Option<BytesField>,
    value: BytesField,
}

#[derive(Deserialize)]
struct TxnOpReq {
    op: String,
    key: BytesField,
    #[serde(default)]
    value: Option<BytesField>,
    #[serde(default)]
    expected: Option<Option<BytesField>>,
}

#[derive(Deserialize)]
struct TxnReq {
    ops: Vec<TxnOpReq>,
}

#[derive(serde::Serialize)]
struct CommitResp {
    committed: bool,
    lsn: u64,
    key_count: usize,
}

#[derive(serde::Serialize)]
struct GetResp {
    key: BytesOut,
    value: BytesOut,
    version: u64,
    read_lsn: u64,
}

impl GetResp {
    fn from_got(key: &[u8], g: Got) -> Self {
        GetResp {
            key: bytes_out(key),
            value: bytes_out(&g.value),
            version: g.version,
            read_lsn: g.read_lsn,
        }
    }
}

#[derive(serde::Serialize)]
struct EntryOut {
    key: BytesOut,
    value: BytesOut,
    version: u64,
}

#[derive(serde::Serialize)]
struct ScanResp {
    read_lsn: u64,
    count: usize,
    entries: Vec<EntryOut>,
}

#[derive(Deserialize)]
struct RangeReq {
    #[serde(default)]
    start: Option<BytesField>,
    #[serde(default)]
    end: Option<BytesField>,
    #[serde(default)]
    prefix: Option<BytesField>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    at_lsn: Option<u64>,
}

// ---------- handlers ----------

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

async fn status(State(s): State<AppState>) -> Result<Json<Status>> {
    Ok(Json(s.store.status().await))
}

async fn get_key(
    State(s): State<AppState>,
    AxumPath(key): AxumPath<String>,
    Query(q): Query<BTreeMap<String, String>>,
) -> Result<Json<GetResp>> {
    let key = path_key(&key);
    let at_lsn = match q.get("lsn") {
        None => None,
        Some(raw) => Some(raw.parse::<u64>().map_err(|_| {
            KvError::InvalidRequest(format!("lsn must be a non-negative integer, got {raw:?}"))
        })?),
    };
    let got = s.store.get_at(&key, at_lsn).await?;
    Ok(Json(GetResp::from_got(&key, got)))
}

async fn put_key(
    State(s): State<AppState>,
    AxumPath(key): AxumPath<String>,
    ApiJson(req): ApiJson<PutReq>,
) -> Result<Json<CommitResp>> {
    let key = path_key(&key);
    let value = req.value.into_bytes("value")?;
    let m = s
        .store
        .build_mutation(vec![(key, WalOp::Put(value))])?;
    let out = s.store.commit(m).await?;
    Ok(Json(CommitResp {
        committed: true,
        lsn: out.lsn,
        key_count: out.key_count,
    }))
}

async fn delete_key(
    State(s): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> Result<Json<CommitResp>> {
    let key = path_key(&key);
    let m = s.store.build_mutation(vec![(key, WalOp::Delete)])?;
    let out = s.store.commit(m).await?;
    Ok(Json(CommitResp {
        committed: true,
        lsn: out.lsn,
        key_count: out.key_count,
    }))
}

async fn cas_key(
    State(s): State<AppState>,
    AxumPath(key): AxumPath<String>,
    ApiJson(req): ApiJson<CasReq>,
) -> Result<Json<CommitResp>> {
    let key = path_key(&key);
    let value = req.value.into_bytes("value")?;
    let expected = match req.expected {
        None => None,
        Some(field) => Some(field.into_bytes("expected")?),
    };
    let m = s.store.build_mutation(vec![(
        key,
        WalOp::Cas { expected, value },
    )])?;
    let out = s.store.commit(m).await?;
    Ok(Json(CommitResp {
        committed: true,
        lsn: out.lsn,
        key_count: out.key_count,
    }))
}

async fn txn(
    State(s): State<AppState>,
    ApiJson(req): ApiJson<TxnReq>,
) -> Result<Json<CommitResp>> {
    let mut ops = Vec::with_capacity(req.ops.len());
    for (i, op) in req.ops.into_iter().enumerate() {
        let key = op.key.into_bytes(&format!("ops[{i}].key"))?;
        let wal_op = match op.op.as_str() {
            "put" => {
                let value = op
                    .value
                    .ok_or_else(|| KvError::InvalidRequest(format!("ops[{i}]: put requires value")))?
                    .into_bytes(&format!("ops[{i}].value"))?;
                WalOp::Put(value)
            }
            "delete" => WalOp::Delete,
            "cas" => {
                let value = op
                    .value
                    .ok_or_else(|| KvError::InvalidRequest(format!("ops[{i}]: cas requires value")))?
                    .into_bytes(&format!("ops[{i}].value"))?;
                let expected = match op.expected {
                    None => None, // field omitted: must be absent
                    Some(None) => None,
                    Some(Some(f)) => Some(f.into_bytes(&format!("ops[{i}].expected"))?),
                };
                WalOp::Cas { expected, value }
            }
            other => {
                return Err(KvError::InvalidRequest(format!(
                    "ops[{i}]: unknown operation type {other:?}, expected put|delete|cas"
                )))
            }
        };
        ops.push((key, wal_op));
    }
    let m = s.store.build_mutation(ops)?;
    let out = s.store.commit(m).await?;
    Ok(Json(CommitResp {
        committed: true,
        lsn: out.lsn,
        key_count: out.key_count,
    }))
}

fn entry_out(e: KvEntry) -> EntryOut {
    EntryOut {
        key: bytes_out(&e.key),
        value: bytes_out(&e.value),
        version: e.version,
    }
}

async fn range_query(
    State(s): State<AppState>,
    ApiJson(req): ApiJson<RangeReq>,
) -> Result<Json<ScanResp>> {
    let start = match req.start {
        Some(f) => Some(f.into_bytes("start")?),
        None => None,
    };
    let end = match req.end {
        Some(f) => Some(f.into_bytes("end")?),
        None => None,
    };
    let (entries, read_lsn) = if let Some(p) = req.prefix {
        let prefix = p.into_bytes("prefix")?;
        s.store.prefix(&prefix, req.limit, req.at_lsn).await?
    } else {
        let (st, en) = (start.as_deref(), end.as_deref());
        s.store.range(st, en, req.limit, req.at_lsn).await?
    };
    Ok(Json(ScanResp {
        read_lsn,
        count: entries.len(),
        entries: entries.into_iter().map(entry_out).collect(),
    }))
}

#[derive(Deserialize)]
struct PrefixQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    lsn: Option<u64>,
}

async fn prefix_query(
    State(s): State<AppState>,
    AxumPath(prefix): AxumPath<String>,
    Query(q): Query<PrefixQuery>,
) -> Result<Json<ScanResp>> {
    let prefix = prefix.as_bytes();
    let (entries, read_lsn) = s.store.prefix(prefix, q.limit, q.lsn).await?;
    Ok(Json(ScanResp {
        read_lsn,
        count: entries.len(),
        entries: entries.into_iter().map(entry_out).collect(),
    }))
}

async fn manual_compact(State(s): State<AppState>) -> Result<Json<serde_json::Value>> {
    let lsn = s.store.compact().await?;
    Ok(Json(json!({ "compacted": true, "snapshot_lsn": lsn })))
}

async fn fallback(req: Request<axum::body::Body>) -> impl IntoResponse {
    KvError::RouteNotFound(format!("{} {}", req.method(), req.uri().path())).into_response()
}

async fn metrics_handler(State(s): State<AppState>) -> impl IntoResponse {
    let m = &s.metrics;
    let status = s.store.status().await;
    let uptime = m.started_at.elapsed().as_secs_f64();
    let routes = m.requests_by_route.lock().await;
    let mut lines = Vec::new();
    lines.push("# HELP kvs_requests_total Total HTTP requests.".into());
    lines.push("# TYPE kvs_requests_total counter".into());
    lines.push(format!(
        "kvs_requests_total {}",
        m.requests_total.load(Ordering::Relaxed)
    ));
    lines.push("# HELP kvs_inflight_requests Requests currently being served.".into());
    lines.push("# TYPE kvs_inflight_requests gauge".into());
    lines.push(format!("kvs_inflight_requests {}", m.inflight.load(Ordering::Relaxed)));
    lines.push("# HELP kvs_errors_5xx_total 5xx responses.".into());
    lines.push("# TYPE kvs_errors_5xx_total counter".into());
    lines.push(format!(
        "kvs_errors_5xx_total {}",
        m.errors_5xx_total.load(Ordering::Relaxed)
    ));
    lines.push("# HELP kvs_key_count Number of live keys.".into());
    lines.push("# TYPE kvs_key_count gauge".into());
    lines.push(format!("kvs_key_count {}", status.key_count));
    lines.push("# HELP kvs_last_lsn Latest committed LSN.".into());
    lines.push("# TYPE kvs_last_lsn gauge".into());
    lines.push(format!("kvs_last_lsn {}", status.last_lsn));
    lines.push("# HELP kvs_wal_size_bytes WAL size in bytes.".into());
    lines.push("# TYPE kvs_wal_size_bytes gauge".into());
    lines.push(format!("kvs_wal_size_bytes {}", status.wal_size_bytes));
    lines.push("# HELP kvs_last_snapshot_lsn LSN of newest snapshot.".into());
    lines.push("# TYPE kvs_last_snapshot_lsn gauge".into());
    lines.push(format!("kvs_last_snapshot_lsn {}", status.last_snapshot_lsn));
    lines.push("# HELP kvs_compacting Whether compaction is running (1/0).".into());
    lines.push("# TYPE kvs_compacting gauge".into());
    lines.push(format!("kvs_compacting {}", u8::from(status.compacting)));
    lines.push("# HELP kvs_uptime_seconds Process uptime.".into());
    lines.push("# TYPE kvs_uptime_seconds gauge".into());
    lines.push(format!("kvs_uptime_seconds {uptime:.3}"));
    for (route, count) in routes.iter() {
        lines.push(format!("kvs_requests_by_route_total{{route=\"{route}\"}} {count}"));
    }
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        lines.join("\n") + "\n",
    )
}

async fn metrics_middleware(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let m = &state.metrics;
    m.requests_total.fetch_add(1, Ordering::Relaxed);
    m.inflight.fetch_add(1, Ordering::Relaxed);
    let route = req
        .uri()
        .path()
        .trim_start_matches('/')
        .split('/')
        .take(2)
        .collect::<Vec<_>>()
        .join("/");
    let response = next.run(req).await;
    m.inflight.fetch_sub(1, Ordering::Relaxed);
    if response.status().is_server_error() {
        m.errors_5xx_total.fetch_add(1, Ordering::Relaxed);
    }
    *m.requests_by_route
        .lock()
        .await
        .entry(route)
        .or_insert(0) += 1;
    response
}
