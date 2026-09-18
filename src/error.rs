//! Structured error types shared by the storage engine and the HTTP layer.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("key not found: {0}")]
    KeyNotFound(String),

    #[error("version not found at LSN {0}")]
    VersionNotFound(u64),

    #[error("CAS conflict on key '{0}': current value does not match expected value")]
    CasConflict(String),

    #[error("empty key is not allowed")]
    EmptyKey,

    #[error("key size {size} bytes exceeds limit of {limit} bytes")]
    KeyTooLarge { size: usize, limit: usize },

    #[error("value size {size} bytes exceeds limit of {limit} bytes")]
    ValueTooLarge { size: usize, limit: usize },

    #[error("transaction contains no operations")]
    EmptyTransaction,

    #[error("transaction contains {count} operations, limit is {limit}")]
    TooManyOperations { count: usize, limit: usize },

    #[error("transaction contains contradictory operations on key '{0}'")]
    ContradictoryKey(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("invalid base64 field '{field}': {message}")]
    InvalidBase64 { field: String, message: String },

    #[error("invalid UTF-8 field '{0}'")]
    InvalidUtf8(String),

    #[error("route not found: {0}")]
    RouteNotFound(String),

    #[error("WAL record corrupted at offset {offset}: {message}")]
    CorruptWal { offset: u64, message: String },

    #[error("snapshot corrupted: {0}")]
    CorruptSnapshot(String),

    #[error("serialization error: {0}")]
    Serialize(String),

    #[error("I/O error: {0}")]
    Io(String),
}

impl From<std::io::Error> for KvError {
    fn from(e: std::io::Error) -> Self {
        KvError::Io(e.to_string())
    }
}

impl From<bincode::Error> for KvError {
    fn from(e: bincode::Error) -> Self {
        KvError::Serialize(e.to_string())
    }
}

/// Stable machine-readable error code returned in the JSON body.
impl KvError {
    pub fn code(&self) -> &'static str {
        match self {
            KvError::KeyNotFound(_) => "KEY_NOT_FOUND",
            KvError::VersionNotFound(_) => "VERSION_NOT_FOUND",
            KvError::CasConflict(_) => "CAS_CONFLICT",
            KvError::EmptyKey => "EMPTY_KEY",
            KvError::KeyTooLarge { .. } => "KEY_TOO_LARGE",
            KvError::ValueTooLarge { .. } => "VALUE_TOO_LARGE",
            KvError::EmptyTransaction => "EMPTY_TRANSACTION",
            KvError::TooManyOperations { .. } => "TOO_MANY_OPERATIONS",
            KvError::ContradictoryKey(_) => "CONTRADICTORY_KEY",
            KvError::InvalidRequest(_) => "INVALID_REQUEST",
            KvError::InvalidConfig(_) => "INVALID_CONFIG",
            KvError::InvalidBase64 { .. } => "INVALID_BASE64",
            KvError::InvalidUtf8(_) => "INVALID_UTF8",
            KvError::RouteNotFound(_) => "ROUTE_NOT_FOUND",
            KvError::CorruptWal { .. } => "CORRUPT_WAL",
            KvError::CorruptSnapshot(_) => "CORRUPT_SNAPSHOT",
            KvError::Serialize(_) => "SERIALIZE_ERROR",
            KvError::Io(_) => "IO_ERROR",
        }
    }

    pub fn http_status(&self) -> StatusCode {
        match self {
            KvError::KeyNotFound(_) | KvError::RouteNotFound(_) => StatusCode::NOT_FOUND,
            KvError::CasConflict(_) => StatusCode::CONFLICT,
            KvError::VersionNotFound(_) => StatusCode::NOT_FOUND,
            KvError::CorruptWal { .. }
            | KvError::CorruptSnapshot(_)
            | KvError::Io(_)
            | KvError::Serialize(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }
}

#[derive(Serialize)]
pub struct ErrorBody {
    pub error: &'static str,
    pub code: &'static str,
    pub message: String,
}

impl IntoResponse for KvError {
    fn into_response(self) -> Response {
        let status = self.http_status();
        let body = ErrorBody {
            error: "kv_error",
            code: self.code(),
            message: self.to_string(),
        };
        (status, Json(body)).into_response()
    }
}
