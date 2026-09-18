//! End-to-end tests over the real HTTP router (no network listener: the
//! tower::Service is driven directly with one-shot requests).

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::Router;
use http_body_util::BodyExt;
use kvstore::api::app;
use kvstore::store::Store;
use tempfile::TempDir;
use tower::util::ServiceExt;

use axum::http::{Request, StatusCode};

fn http_app() -> (Router, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = Store::open(
        dir.path().to_path_buf(),
        4 * 1024 * 1024,
        kvstore::config::Limits {
            max_key_bytes: 64 * 1024,
            max_value_bytes: 16 * 1024 * 1024,
            max_ops_per_txn: 1024,
        },
    )
    .unwrap();
    (app(Arc::new(store)), dir)
}

async fn request(
    app: &Router,
    method: &str,
    uri: &str,
    json_body: Option<&str>,
) -> (StatusCode, serde_json::Value, String) {
    let mut builder = Request::builder().method(method).uri(uri);
    if json_body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let body = match json_body {
        Some(j) => Body::from(j.to_string()),
        None => Body::empty(),
    };
    let resp = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let json = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    (status, json, text)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_http_workflow() {
    let (app, _dir) = http_app();

    // 404 on missing key.
    let (st, j, _raw) = request(&app, "GET", "/v1/kv/missing", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(j["found"], false);
    assert_eq!(j["value"], serde_json::Value::Null);
    assert_eq!(j["read_lsn"], 0);

    // Put.
    let (st, j, _raw) = request(&app, "PUT", "/v1/kv/a", Some(r#"{"value":"1"}"#)).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(j["lsn"], 1);

    // Get it back.
    let (st, j, _raw) = request(&app, "GET", "/v1/kv/a", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(j["found"], true);
    assert_eq!(j["value"], "1");
    assert_eq!(j["version"], 1);

    // CAS mismatch -> 409 structured conflict.
    let (st, j, _raw) = request(
        &app,
        "PUT",
        "/v1/kv/a/cas",
        Some(r#"{"expected":"wrong","value":"2"}"#),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(j["error"]["kind"], "cas_conflict");
    assert_eq!(j["error"]["expected"], "wrong");
    assert_eq!(j["error"]["actual"], "1");
    assert_eq!(j["error"]["key"], "a");

    // CAS match.
    let (st, j, _raw) = request(
        &app,
        "PUT",
        "/v1/kv/a/cas",
        Some(r#"{"expected":"1","value":"2"}"#),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(j["version"], 2);

    // Explicit JSON null means "key must be absent".
    let (st, j, _raw) = request(
        &app,
        "PUT",
        "/v1/kv/fresh/cas",
        Some(r#"{"expected":null,"value":"born"}"#),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(j["version"], 3);
    // Repeating the absent-expectation now conflicts.
    let (st, j, _raw) = request(
        &app,
        "PUT",
        "/v1/kv/fresh/cas",
        Some(r#"{"expected":null,"value":"again"}"#),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(j["error"]["kind"], "cas_conflict");
    assert_eq!(j["error"]["expected"], serde_json::Value::Null);
    assert_eq!(j["error"]["actual"], "born");

    // Atomic transaction: both CAS ops commit together.
    let (st, j, _raw) = request(
        &app,
        "POST",
        "/v1/txn",
        Some(
            r#"{"ops":[
                {"op":"put","key":"b","value":"10"},
                {"op":"cas","key":"a","expected":"2","value":"3"}
            ]}"#,
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(j["lsn"], 4);
    assert_eq!(j["effects"].as_array().unwrap().len(), 2);

    // Aborting transaction: b is "10", expected mismatch -> nothing applied.
    let (st, j, _raw) = request(
        &app,
        "POST",
        "/v1/txn",
        Some(
            r#"{"ops":[
                {"op":"put","key":"c","value":"x"},
                {"op":"cas","key":"b","expected":"999","value":"11"}
            ]}"#,
        ),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(j["error"]["kind"], "cas_conflict");
    let (st, j, _raw) = request(&app, "GET", "/v1/kv/c", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(j["found"], false);

    // Prefix scan.
    let (st, j, _raw) = request(&app, "GET", "/v1/kv?prefix=a", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(j["count"], 1);
    assert_eq!(j["entries"][0]["key"], "a");
    assert_eq!(j["entries"][0]["value"], "3");

    // Delete.
    let (st, j, _raw) = request(&app, "DELETE", "/v1/kv/b", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(j["changed"], true);

    // Status + metrics + health.
    let (st, j, _raw) = request(&app, "GET", "/v1/status", None).await;
    assert_eq!(st, StatusCode::OK);
    // Commits: put a, cas a, fresh-cas, txn{b,a}, delete b; the two
    // failed CAS attempts consumed no LSN.
    assert_eq!(j["lsn"], 5);
    assert_eq!(j["key_count"], 2);
    assert_eq!(j["limits"]["max_ops_per_txn"], 1024);

    let (st, _, raw) = request(&app, "GET", "/metrics", None).await;
    assert_eq!(st, StatusCode::OK);
    assert!(raw.contains("kvstore_lsn 5"));
    assert!(raw.contains("kvstore_commits_total 5"));
    let (st, _, raw) = request(&app, "GET", "/healthz", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(raw, "ok");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structured_validation_errors() {
    let (app, _dir) = http_app();

    // Empty body.
    let (st, j, _raw) = request(&app, "PUT", "/v1/kv/a", Some("")).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(j["error"]["kind"], "invalid_request");

    // Malformed JSON.
    let (st, j, _raw) = request(&app, "PUT", "/v1/kv/a", Some("{not json")).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(j["error"]["kind"], "invalid_request");

    // Missing value field.
    let (st, j, _raw) = request(&app, "PUT", "/v1/kv/a", Some("{}")).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(j["error"]["kind"], "invalid_request");

    // Empty transaction.
    let (st, j, _raw) = request(&app, "POST", "/v1/txn", Some(r#"{"ops":[]}"#)).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(j["error"]["kind"], "empty_transaction");

    // Duplicate key in transaction.
    let (st, j, _raw) = request(
        &app,
        "POST",
        "/v1/txn",
        Some(r#"{"ops":[{"op":"put","key":"k","value":"1"},{"op":"delete","key":"k"}]}"#),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(j["error"]["kind"], "duplicate_key_in_transaction");

    // CAS without explicit expected.
    let (st, j, _raw) = request(&app, "PUT", "/v1/kv/k/cas", Some(r#"{"value":"1"}"#)).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(j["error"]["kind"], "invalid_request");

    // Unknown op type.
    let (st, j, _raw) = request(
        &app,
        "POST",
        "/v1/txn",
        Some(r#"{"ops":[{"op":"frobnicate","key":"k"}]}"#),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(j["error"]["kind"], "invalid_request");

    // Scan without parameters.
    let (st, _, _raw) = request(&app, "GET", "/v1/kv", None).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // Unknown endpoint.
    let resp = app
        .clone()
        .oneshot(Request::builder().uri("/nope").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn range_scan_and_compaction_endpoint() {
    let (app, _dir) = http_app();
    for k in ["a1", "a2", "b1"] {
        request(
            &app,
            "PUT",
            &format!("/v1/kv/{k}"),
            Some(r#"{"value":"v"}"#),
        )
        .await;
    }
    let (st, j, _raw) = request(&app, "GET", "/v1/kv?start=a1&end=b1", None).await;
    assert_eq!(st, StatusCode::OK);
    let keys: Vec<String> = j["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["key"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(keys, vec!["a1".to_string(), "a2".to_string()]);

    let (st, j, _raw) = request(&app, "POST", "/v1/compact?wait=true", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(j["snapshot_lsn"], 3);
}
