//! End-to-end HTTP tests via axum's in-process `oneshot`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use memtxn_kvs::config::Config;
use memtxn_kvs::server::router;
use memtxn_kvs::store::Store;
use serde_json::Value;
use tempfile::TempDir;
use tower::util::ServiceExt;

fn cfg(dir: &std::path::Path) -> Config {
    Config {
        data_dir: dir.to_str().unwrap().into(),
        listen_addr: "127.0.0.1:0".into(),
        max_key_bytes: 1024,
        max_value_bytes: 1024,
        max_txn_ops: 64,
        compaction_threshold_bytes: 10_000_000,
    }
}

async fn app(dir: &TempDir) -> axum::Router {
    router(Store::open(cfg(dir.path())).await.unwrap())
}

async fn req(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let builder = Request::builder().method(method).uri(uri);
    let req = match body {
        Some(v) => builder
            .header("content-type", "application/json")
            .body(Body::from(v.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

#[tokio::test]
async fn put_get_delete_and_404() {
    let dir = TempDir::new().unwrap();
    let app = app(&dir).await;

    let (s, j) = req(
        app.clone(),
        "PUT",
        "/v1/kv/hello",
        Some(serde_json::json!({"value": "world"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["lsn"], 1);

    let (s, j) = req(app.clone(), "GET", "/v1/kv/hello", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["value"]["text"], "world");
    assert_eq!(j["version"], 1);
    assert_eq!(j["read_lsn"], 1);

    let (s, _) = req(app.clone(), "GET", "/v1/kv/missing", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    let (s, j) = req(app.clone(), "DELETE", "/v1/kv/hello", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["lsn"], 2);

    let (s, j) = req(app, "GET", "/v1/kv/hello", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert_eq!(j["code"], "KEY_NOT_FOUND");
}

#[tokio::test]
async fn cas_returns_explicit_conflict() {
    let dir = TempDir::new().unwrap();
    let app = app(&dir).await;
    let (s, _) = req(
        app.clone(),
        "PUT",
        "/v1/kv/cfg",
        Some(serde_json::json!({"value": "v1"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // Wrong expected value -> 409 structured error.
    let (s, j) = req(
        app.clone(),
        "POST",
        "/v1/kv/cfg/cas",
        Some(serde_json::json!({"expected": "WRONG", "value": "v2"})),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert_eq!(j["code"], "CAS_CONFLICT");
    // Value untouched.
    let (_, j) = req(app.clone(), "GET", "/v1/kv/cfg", None).await;
    assert_eq!(j["value"]["text"], "v1");

    // Correct CAS succeeds.
    let (s, j) = req(
        app,
        "POST",
        "/v1/kv/cfg/cas",
        Some(serde_json::json!({"expected": "v1", "value": "v2"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["committed"], true);
}

#[tokio::test]
async fn txn_commit_then_prefix_and_range() {
    let dir = TempDir::new().unwrap();
    let app = app(&dir).await;
    let body = serde_json::json!({
        "ops": [
            {"op": "put", "key": "user:1", "value": {"base64": "YWxpY2U="}},
            {"op": "put", "key": "user:2", "value": "bob"},
            {"op": "put", "key": "order:1", "value": "42"}
        ]
    });
    let (s, j) = req(app.clone(), "POST", "/v1/txn", Some(body)).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["lsn"], 1);

    let (s, j) = req(app.clone(), "GET", "/v1/prefix/user:", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["count"], 2);
    assert_eq!(j["read_lsn"], 1);

    let (s, j) = req(
        app,
        "POST",
        "/v1/range",
        Some(serde_json::json!({"start": "order:", "end": "p"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["entries"][0]["key"]["text"], "order:1");
    assert_eq!(j["entries"][0]["value"]["text"], "42");
}

#[tokio::test]
async fn rejected_txn_returns_distinguishable_error() {
    let dir = TempDir::new().unwrap();
    let app = app(&dir).await;
    let body = serde_json::json!({"ops": [
        {"op": "put", "key": "k", "value": "a"},
        {"op": "delete", "key": "k"}
    ]});
    let (s, j) = req(app, "POST", "/v1/txn", Some(body)).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(j["code"], "CONTRADICTORY_KEY");
}

#[tokio::test]
async fn bad_inputs_are_structured_errors() {
    let dir = TempDir::new().unwrap();
    let app = app(&dir).await;

    // Malformed JSON body.
    let request = Request::builder()
        .method("PUT")
        .uri("/v1/kv/x")
        .header("content-type", "application/json")
        .body(Body::from("{not json"))
        .unwrap();
    let resp = app.clone().oneshot(request).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let j: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(j["code"], "INVALID_REQUEST");

    // Empty body.
    let (s, j) = req(app.clone(), "PUT", "/v1/kv/x", None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(j["code"], "INVALID_REQUEST");

    // Unknown op type in transaction.
    let (s, j) = req(
        app.clone(),
        "POST",
        "/v1/txn",
        Some(serde_json::json!({"ops": [{"op": "frobnicate", "key": "k"}]})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(j["code"], "INVALID_REQUEST");

    // Invalid base64.
    let (s, j) = req(
        app.clone(),
        "PUT",
        "/v1/kv/x",
        Some(serde_json::json!({"value": {"base64": "!!!not-b64"}})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(j["code"], "INVALID_BASE64");

    // Unknown route.
    let (s, j) = req(app, "GET", "/nope", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert_eq!(j["code"], "ROUTE_NOT_FOUND");
}

#[tokio::test]
async fn status_and_metrics_endpoints() {
    let dir = TempDir::new().unwrap();
    let app = app(&dir).await;
    let (s, _) = req(
        app.clone(),
        "PUT",
        "/v1/kv/a",
        Some(serde_json::json!({"value": "1"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, j) = req(app.clone(), "GET", "/v1/status", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["last_lsn"], 1);
    assert_eq!(j["key_count"], 1);
    assert_eq!(j["compacting"], false);
    assert!(j["compaction_threshold_bytes"].as_u64().unwrap() > 0);

    let request = Request::builder()
        .method("GET")
        .uri("/v1/metrics")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(request).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(text.contains("kvs_key_count 1"));
    assert!(text.contains("kvs_last_lsn 1"));
    assert!(text.contains("kvs_requests_total"));
}

#[tokio::test]
async fn snapshot_read_at_past_lsn() {
    let dir = TempDir::new().unwrap();
    let app = app(&dir).await;
    for v in ["v1", "v2", "v3"] {
        let (s, _) = req(
            app.clone(),
            "PUT",
            "/v1/kv/k",
            Some(serde_json::json!({"value": v})),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
    }
    let (s, j) = req(app, "GET", "/v1/kv/k?lsn=2", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["value"]["text"], "v2");
    assert_eq!(j["version"], 2);
    assert_eq!(j["read_lsn"], 2);
}

#[tokio::test]
async fn binary_key_and_value_supported() {
    let dir = TempDir::new().unwrap();
    let app = app(&dir).await;
    let body = serde_json::json!({
        "ops": [{"op": "put", "key": {"base64": "AAEBBQ=="}, "value": {"base64": "////"}}]
    });
    let (s, _) = req(app.clone(), "POST", "/v1/txn", Some(body)).await;
    assert_eq!(s, StatusCode::OK);
    // Percent-encoded raw key in path: AA 01 01 05 -> %00%01%01%05
    let (s, j) = req(app, "GET", "/v1/kv/%00%01%01%05", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["value"]["base64"], "////");
}
