# memtxn-kvs — 可独立部署的内存事务型键值存储服务

单机、纯后端的内存事务型 KV 服务。上层业务通过 HTTP 把读写请求发给它，它在高并发下
保证多键事务原子性与隔离性，并通过预写日志（WAL）+ 快照在进程重启后恢复到崩溃前
**已确认提交**的状态。

- 语言/运行时：Rust + Tokio
- HTTP 框架：axum
- 持久化：本地磁盘 WAL（CRC32 校验帧，fsync 后才应答提交）+ 全量快照
- 非分布式、无账户体系、无前端页面

---

## 1. 一键构建与启动

### 方式 A：Docker Compose（推荐，一条命令）

```bash
docker compose up --build -d
# 服务监听 http://localhost:8080 ，数据在 docker volume kvs-data 内
```

### 方式 B：Docker

```bash
docker build -t memtxn-kvs .
docker run -d -p 8080:8080 -v "$(pwd)/data:/data" --name kvs memtxn-kvs
```

### 方式 C：本地 cargo

```bash
cargo build --release
DATA_DIR=./data LISTEN_ADDR=0.0.0.0:8080 ./target/release/kvs
```

健康检查：`GET /health` → `{"status":"ok"}`

### 预置算例（可直接调用）

```bash
# 服务启动后
scripts/demo.sh                 # 默认 http://127.0.0.1:8080
# 或容器内：
docker exec -it memtxn-kvs /usr/local/bin/demo.sh
```

算例内容：3 次顺序写入 → 一个多键原子事务（两个账户同批生效）→ 双 CAS 保护的
原子转账（70/80，总和守恒）→ 一次预期失败的 CAS（返回 409 `CAS_CONFLICT`）→
前缀/区间扫描 → 指定 LSN 的快照读 → 运行状态。

---

## 2. 配置（全部在启动时校验，非法值直接拒绝启动，exit code 2）

| 环境变量 | 默认值 | 含义 | 合法性 |
|---|---|---|---|
| `DATA_DIR` | `./data` | WAL 与快照目录 | 非空 |
| `LISTEN_ADDR` | `0.0.0.0:8080` | 监听地址 | 合法 socket 地址 |
| `MAX_KEY_BYTES` | `65536` | 单键最大字节数 | > 0 |
| `MAX_VALUE_BYTES` | `1048576` | 单值最大字节数 | > 0 |
| `MAX_TXN_OPS` | `1024` | 单事务最大操作条数 | > 0 |
| `COMPACTION_THRESHOLD_BYTES` | `16777216` | WAL 达到该字节数触发快照压缩 | > 0 |

键/值可以是任意二进制：HTTP 报文中用 UTF-8 字符串或 `{"base64":"..."}` 表达；
响应同时返回 `text`（若为合法 UTF-8）、`base64`、`bytes_len`。

---

## 3. HTTP 接口

所有错误均为结构化 JSON：`{"error":"kv_error","code":"<机器可读码>","message":"..."}`，
并带合适的 HTTP 状态码。

### 单键

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/v1/kv/{key}` | 读最新已提交值；支持 `?lsn=N` 读版本快照视图 |
| `PUT` | `/v1/kv/{key}` | 写，body：`{"value": "..."}` |
| `DELETE` | `/v1/kv/{key}` | 删除（记一条 Delete 版本，后续可被历史快照读看到） |
| `POST` | `/v1/kv/{key}/cas` | 比较后写入，body：`{"expected": "..." 或 null, "value": "..."}` |

CAS 语义：仅当键的**当前已提交值**等于 `expected`（`null` 表示键必须不存在）时才写入；
否则返回 `409` 与错误码 `CAS_CONFLICT`，绝不静默失败、绝不误报成功。

读取响应包含：
- `version`：该键当前值的版本号（= 产生该值的 LSN，键内严格单调递增）
- `read_lsn`：本次读取对应的日志位置，调用方可据此判断数据新旧

### 多键事务

`POST /v1/txn`

```json
{
  "ops": [
    {"op": "put",    "key": "acct:alice", "value": "100"},
    {"op": "cas",    "key": "acct:bob",   "expected": "50", "value": "80"},
    {"op": "delete", "key": "scratch"}
  ]
}
```

- 全有或全无：任一 CAS 前置条件不满足 → 整笔回滚，返回 `CAS_CONFLICT`，已做改动不可见。
- 同一事务内对**同一个键出现多次操作**属于矛盾事务，提交前以 `CONTRADICTORY_KEY` 拒绝。
- 操作数为 0 → `EMPTY_TRANSACTION`；超过 `MAX_TXN_OPS` → `TOO_MANY_OPERATIONS`。
- 成功返回 `{"committed":true,"lsn":N,"key_count":K}`，批次内所有改动在同一 LSN 生效。

### 查询

- 前缀：`GET /v1/prefix/{prefix}?limit=&lsn=`
- 区间/前缀：`POST /v1/range`
  ```json
  {"start": "a", "end": "z", "limit": 100, "lsn": 50}
  ```
  `start`/`end` 均可省（开区间），也可传 `prefix` 做前缀查询。结果反映调用时刻
  （或指定 `lsn`）已提交的数据，含每项 `version`，顶层含 `read_lsn`。

### 运维

| 路径 | 说明 |
|---|---|
| `GET /v1/status` | 键总数 `key_count`、日志位置 `last_lsn`、WAL 字节数、最近快照位置 `last_snapshot_lsn`、是否正在压缩 `compacting`、压缩阈值 |
| `GET /v1/metrics` | Prometheus 文本指标（请求数/在途数/5xx/键数/LSN/WAL 大小/快照位置/压缩标志/uptime） |
| `POST /v1/admin/compact` | 手动触发一次快照压缩 |
| `GET /health` | 存活探针 |

错误码一览：`KEY_NOT_FOUND(404)`、`CAS_CONFLICT(409)`、`VERSION_NOT_FOUND(404)`、
`EMPTY_KEY`、`KEY_TOO_LARGE`、`VALUE_TOO_LARGE`、`EMPTY_TRANSACTION`、
`TOO_MANY_OPERATIONS`、`CONTRADICTORY_KEY`、`INVALID_REQUEST`、`INVALID_BASE64`、
`ROUTE_NOT_FOUND(404)`、`CORRUPT_WAL`、`CORRUPT_SNAPSHOT`、`IO_ERROR` 等（后三类 500）。

---

## 4. 并发与一致性保证

- **提交串行化**：所有事务通过单一 commit gate 串行进入“校验前置条件 → 追加并
  fsync WAL → 一次性应用到内存表”的临界区。CAS 的判定与写入在同一临界区内原子完成，
  因此并发 CAS 恰好一个胜出，其余得到明确冲突。
- **读已提交 / 快照读**：读走 `RwLock` 共享锁，互不阻塞；只在整笔事务应用完成后才能
  看到该事务，读不到任何未提交中间态。每个键维护版本链，二分定位 `lsn <= 请求位置`
  的版本，实现带版本号的快照视图。
- **版本号单调**：LSN 是无空洞、严格递增的全局提交序号；同一键的版本号严格递增、
  不倒退、不重复。重启后从“快照 LSN + WAL 重放”继续编号。

## 5. 持久化、压缩与恢复

- **WAL 先写盘**：提交在 `write + fsync` 完成后才向客户端返回已提交。帧格式
  `u32 len | u32 crc32 | bincode 负载`，崩溃产生的半截尾帧在打开时丢弃（该尾帧从未
  fsync，也就从未被确认提交）。
- **快照压缩**：WAL 达到阈值时后台任务执行：短暂持共享锁克隆全量状态 → 写
  `snapshot.bin.tmp` 并 fsync → 原子 rename 覆盖旧快照（期间读写不中断）→ 仅在最后
  截断重写 WAL 时短暂串行提交。快照之后新提交的记录会保留在新 WAL 中。
- **恢复顺序**：加载最近快照（含每键版本历史）→ 校验并重放其后 WAL
  （LSN 必须从 `snapshot_lsn+1` 连续无重复；重复/空洞/CRC 错误按损坏处理）。
  即使崩溃发生在“快照已发布、WAL 尚未截断”的窗口，WAL 中被快照覆盖的旧前缀也会被
  识别并剔除，**同一条已提交记录绝不重复应用**。
- 未通过提交（CAS 失败/矛盾事务等）从不写 WAL，故不会在恢复后变成“已提交”。

## 6. 测试

```bash
cargo test
```

覆盖（单元 + 引擎集成 + HTTP 端到端）：

- 多键事务全有或全无（失败 CAS 回滚同批其他写；成功事务同 LSN 原子可见）
- 高并发事务相互隔离（6 worker × 100 次双 CAS 转账 + 2000 次并发读，余额总和恒为 200）
- 并发 CAS 语义（32 个竞争者恰好 1 个胜出、31 个显式冲突；1000 次跨 50 并发的
  CAS 自增最终精确等于 1000，无丢失更新）
- 版本号单调递增（put/delete/put、快照读、重启后继续递增）
- 快照 + WAL 重启恢复（含多次压缩、快照发布/日志截断崩溃窗口）
- WAL 不重复应用（恢复后下一个 LSN 精确接续；陈旧前缀注入被剔除）
- 未提交改动不恢复、非法配置启动拒绝、矛盾事务/超限操作提交前拒绝
- HTTP 结构化错误码、二进制键值、status / Prometheus 指标

## 7. 目录结构

```
src/
  main.rs       启动入口（启动时校验配置、优雅退出）
  lib.rs
  config.rs     配置与启动期合法性校验
  error.rs      统一结构化错误（错误码 + HTTP 状态）
  wal.rs        预写日志：帧编解码、fsync 追加、重放校验、原子重写
  snapshot.rs   快照：CRC 校验、临时文件 + 原子 rename
  store.rs      内存表、版本链、事务提交门、快照读、压缩与恢复
  server.rs     axum 路由、键值编解码、指标
tests/
  engine.rs     引擎行为测试
  http_api.rs   HTTP 端到端测试
scripts/demo.sh 预置算例
Dockerfile  docker-compose.yml
```
