# HTTP API 参考

所有接口返回 `application/json`（`/healthz` 与 `/metrics` 除外）。
键通过 URL 路径传递，值在 JSON body 中传递。二进制值可用 `value_base64` 传入；
读取时合法 UTF-8 直接以字符串返回，否则返回 `"base64:<...>"`。

所有读取响应都带有当前已提交日志位置：

- 响应头：`X-Read-LSN: <u64>`
- JSON 字段：`read_lsn`

`version` 是该键最后一次被修改时所属事务的 LSN；对同一个键严格单调递增。

## 读单键

```
GET /v1/kv/:key
```

存在：

```json
{"key":"a","found":true,"value":"1","version":2,"read_lsn":5}
```

不存在：

```json
{"key":"missing","found":false,"value":null,"read_lsn":5}
```

## 写单键

```
PUT /v1/kv/:key
Content-Type: application/json

{"value":"hello"}
```

或二进制：

```json
{"value_base64":"aGVsbG8="}
```

响应：

```json
{"lsn":6,"version":6,"changed":false}
```

`changed=true` 表示覆盖了已有值。

## 删除

```
DELETE /v1/kv/:key
```

```json
{"lsn":7,"version":7,"changed":true}
```

`changed=false` 表示键本来就不存在（删除仍消耗一个 LSN，保证后续重建版本更高）。

## 比较后写入（CAS / 条件写入）

```
PUT /v1/kv/:key/cas
Content-Type: application/json

{"expected":"70","value":"71"}
```

- `expected` **必须显式提供**：
  - 字符串：仅当键的当前字节值与它完全相等时写入；
  - `null`：仅当键当前不存在时写入（用于原子创建）。
- 条件不满足返回 **HTTP 409**，不静默成功、不消耗 LSN：

```json
{
  "error": {
    "kind": "cas_conflict",
    "message": "compare-and-swap conflict on key 'a': current value does not match expected",
    "key": "a",
    "expected": "70",
    "actual": "71"
  }
}
```

## 多键原子事务

```
POST /v1/txn
Content-Type: application/json

{
  "ops": [
    {"op": "put",    "key": "a", "value": "1"},
    {"op": "delete", "key": "b"},
    {"op": "cas",    "key": "c", "expected": null, "value": "x"},
    {"op": "put",    "key": "d", "value_base64": "AA=="}
  ]
}
```

规则：

- 至少 1 条、至多 `MAX_OPS_PER_TXN` 条（默认 1024）。
- 同一事务内同一个键只能出现一次，否则 400 `duplicate_key_in_transaction`。
- 任一 `cas` 前置条件不满足 → 整事务 409 回滚，其他操作的改动不可见、不消耗 LSN。
- 成功时所有改动用同一个 LSN 一次性发布：

```json
{
  "lsn": 8,
  "effects": [
    {"op":"put","key":"a","version":8,"changed":true,"replaced":true},
    {"op":"delete","key":"b","version":null,"changed":true,"replaced":false},
    {"op":"cas","key":"c","version":8,"changed":true,"replaced":false}
  ]
}
```

## 前缀查询

```
GET /v1/kv?prefix=user:&limit=1000
```

空前缀（`prefix=`）返回全部键。结果按键字典序排列：

```json
{
  "read_lsn": 8,
  "count": 2,
  "entries": [
    {"key":"user:1:balance","value":"70","version":5},
    {"key":"user:1:name","value":"alice","version":1}
  ]
}
```

## 区间查询

```
GET /v1/kv?start=user:1:name&end=user:2:balance
```

- 默认区间为 `[start, end)`（含起点、不含终点）；
- `end_inclusive=true` 使终点包含；
- `start_exclusive=true` 使起点排除；
- 省略 `end` 表示到最后一个键；
- `limit` 默认 1000，最大 100000，必须 ≥ 1。

`prefix` 与 `start` 互斥；两者都不给返回 400。

## 手动触发快照压缩

```
POST /v1/compact          # 后台执行，立即返回 {"started":true,"snapshot_lsn":null}
POST /v1/compact?wait=true  # 等待完成，返回快照 LSN
```

压缩进行中再次触发返回 409 `compaction_in_progress`。

## 运行状态

```
GET /v1/status
```

```json
{
  "key_count": 8,
  "lsn": 8,
  "snapshot_lsn": 7,
  "wal_total_bytes": 66,
  "wal_active_segment": 2,
  "compacting": false,
  "limits": {
    "max_key_bytes": 65536,
    "max_value_bytes": 16777216,
    "max_ops_per_txn": 1024
  }
}
```

## 指标

`GET /metrics` 返回 Prometheus 文本格式，包含：

- `kvstore_uptime_seconds`
- `kvstore_http_requests_total{status_class="2xx|4xx|5xx"}`
- `kvstore_commits_total`、`kvstore_committed_ops_total`
- `kvstore_cas_conflicts_total`、`kvstore_rejected_transactions_total`
- `kvstore_reads_total`、`kvstore_scans_total`
- `kvstore_compactions_total`、`kvstore_compactions_failed_total`、
  `kvstore_last_compaction_duration_seconds`
- `kvstore_wal_bytes_written_total`、`kvstore_snapshot_bytes_written_total`
- `kvstore_keys_current`、`kvstore_lsn`、`kvstore_wal_bytes_current`、
  `kvstore_compacting`、`kvstore_last_snapshot_lsn`

## 错误结构

```json
{"error": {"kind": "<machine-readable>", "message": "<human readable>",
           "key": "...", "expected": "...", "actual": "..."}}
```

| kind | HTTP 状态码 | 含义 |
| --- | --- | --- |
| `invalid_request` | 400 | JSON/参数非法、body 过大（>32 MiB）、未知 op 等 |
| `empty_key` | 400 | 键为空 |
| `key_too_large` | 400 | 键超长 |
| `value_too_large` | 400 | 值超长 |
| `empty_transaction` | 400 | 事务 0 条操作 |
| `transaction_too_large` | 400 | 事务操作数超限 |
| `duplicate_key_in_transaction` | 400 | 事务内同键出现多次 |
| `cas_conflict` | 409 | 条件不满足（含期望值/实际值） |
| `compaction_in_progress` | 409 | 压缩已在进行 |
| `not_found` | 404 | 路径不存在 |
| `corruption` | 500 | 持久化数据损坏 |
| `io_error` | 500 | 磁盘 I/O 故障 |
