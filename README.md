# kvstore — 内存事务型键值存储服务

一个**可独立部署**的纯后端事务型 KV 服务：内存键值表 + WAL 预写日志 + 快照压缩，
基于 Rust / tokio / axum 实现，单机单节点（非分布式）。

支持：

- 单键读 / 写 / 删除 / 比较后写入（CAS，条件写入）
- **多键原子事务**：全有或全无，提交前校验前置条件，提交时 WAL 先落盘再对内存生效
- 事务间串行化隔离：读永远只看到已提交状态，单读/前缀/范围查询都是一致快照
- 每个已提交事务分配严格递增的 **LSN**；每个键的 `version` 是最后修改它的 LSN，
  对同一键严格单调、不倒退、不重复
- **WAL 持久化**：提交前先 `fsync` 追加日志；崩溃重启先加载最近快照，再重放其后日志
- CRC 校验的分段 WAL，崩溃产生的半截（torn）尾帧在打开时截断，不把未提交数据当成已提交
- **快照压缩**：后台写全量快照（临时文件 + 原子 rename），截断已覆盖的旧日志；
  快照在锁外生成，不长时间阻塞读写
- 前缀查询、键区间查询，响应带本次读取对应的日志位置（`read_lsn` / `X-Read-LSN`）
- 结构化错误响应（可区分的 `kind`），启动时严格校验配置
- `/v1/status` 运行状态接口、`/metrics` Prometheus 指标、`/healthz` 存活探针
- 首次启动内置**预置算例**：顺序写入 + 一个多键原子转账事务

---

## 一条命令构建并用 Docker 启动

需要 Docker（含 Compose 插件）：

```bash
docker compose up --build
```

服务监听 `http://localhost:8080`，数据持久化在 Docker volume `kvstore-data`（容器内 `/data`）。

停止并保留数据：

```bash
docker compose down
```

不用 Compose 也可以：

```bash
docker build -t kvstore .
docker run --rm -p 8080:8080 -v "$PWD/data:/data" kvstore
```

## 本地构建运行

```bash
cargo run --release                       # 默认监听 0.0.0.0:8080，数据目录 ./data
DATA_DIR=/data LISTEN_ADDR=0.0.0.0:8080 \
WAL_COMPACT_THRESHOLD=4194304 ./target/release/kvstore
```

> 注：仓库内 `.cargo/config.toml` 仅在 `aarch64-unknown-linux-gnu` 目标下把 linker
> 指向 `/usr/bin/gcc`（开发机默认 `cc` 是受限包装脚本）；其他架构完全不受影响。

## 快速体验预置算例

首次启动（数据目录为空）会自动写入：

1. 顺序单键写：`user:1:name=alice`、`user:1:balance=100`、`user:2:name=bob`、`user:2:balance=50`
2. 一个多键原子事务（带条件）：alice 余额 `100 -> 70`、bob 余额 `50 -> 80`，
   同时写入 `ledger:transfer:1=user:1->user:2:30`

```bash
curl -s localhost:8080/v1/status | jq
curl -s 'localhost:8080/v1/kv?prefix=user:' | jq
curl -s localhost:8080/v1/kv/user:1:balance | jq
# {"key":"user:1:balance","found":true,"value":"70","version":5,"read_lsn":5}
```

也可运行 `./scripts/demo.sh`（自动启动本地 release 服务并演示完整流程）。

---

## HTTP 接口

详见 [docs/API.md](docs/API.md)。所有错误均为统一结构：

```json
{"error": {"kind": "cas_conflict", "message": "...", "key": "...",
           "expected": "70", "actual": "71"}}
```

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 存活探针 |
| GET | `/metrics` | Prometheus 文本指标 |
| GET | `/v1/status` | 键总数、当前 LSN、快照 LSN、WAL 大小、是否压缩中 |
| GET | `/v1/kv/:key` | 读单键（响应头 `X-Read-LSN` 为读取位置） |
| PUT | `/v1/kv/:key` | 写单键 |
| DELETE | `/v1/kv/:key` | 删除单键 |
| PUT | `/v1/kv/:key/cas` | 比较后写入（`expected` 显式给出，`null` 表示键必须不存在） |
| POST | `/v1/txn` | 多键原子事务（`put` / `delete` / `cas`） |
| GET | `/v1/kv?prefix=..` | 前缀查询 |
| GET | `/v1/kv?start=..&end=..` | 区间查询（`[start,end)`，可 `end_inclusive`） |
| POST | `/v1/compact?wait=true` | 触发快照压缩（默认后台执行） |

错误 `kind` 包括：`empty_key`、`key_too_large`、`value_too_large`、
`empty_transaction`、`transaction_too_large`、`duplicate_key_in_transaction`、
`cas_conflict`（HTTP 409）、`compaction_in_progress`、`corruption`、`io_error`、
`invalid_request`、`not_found`。

### 事务示例

```bash
curl -s -X POST localhost:8080/v1/txn \
  -H 'content-type: application/json' \
  -d '{"ops":[
    {"op":"cas","key":"user:1:balance","expected":"100","value":"70"},
    {"op":"cas","key":"user:2:balance","expected":"50","value":"80"},
    {"op":"put","key":"ledger:transfer:1","value":"user:1->user:2:30"}
  ]}'
```

任一 `cas` 的 `expected` 与当前值不符，整个事务返回 409 且**不产生任何可见改动、不消耗 LSN**。
同一事务内对同一个键出现两次操作会在提交前以 `duplicate_key_in_transaction` 拒绝。

---

## 配置（环境变量，启动时校验）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `DATA_DIR` | `./data` | WAL 与快照目录（不能为空） |
| `LISTEN_ADDR` | `0.0.0.0:8080` | HTTP 监听地址 |
| `WAL_COMPACT_THRESHOLD` | `4194304`（4 MiB） | WAL 总字节达到该值即触发后台快照压缩；**必须 > 0** |
| `MAX_KEY_BYTES` | `65536` | 键最大字节数；**必须 > 0**（键也不能为空） |
| `MAX_VALUE_BYTES` | `16777216` | 值最大字节数；**必须 > 0** |
| `MAX_OPS_PER_TXN` | `1024` | 单事务最大操作条数；**必须 > 0** |
| `SEED_ON_FRESH` | `true` | 空数据目录首次启动时写入预置算例 |
| `RUST_LOG` | `info` | 日志级别 |

任何非法配置（0、负数、非数字、空地址/目录等）都会在启动时直接拒绝退出，
而不会留到运行时才报错。

---

## 持久化与崩溃恢复语义

- 提交顺序 = LSN 顺序 = WAL 追加顺序。事务记录在返回客户端**之前**完成 `write + fsync`。
- 记录用 `magic + len + payload + crc32` 分帧；崩溃导致的半截尾帧在下次打开时被截断，
  因此只会丢失"未确认提交"的尾巴，已确认的提交不丢。
- WAL 按大小分段（`wal-<index>`），仅活动段可追加。
- 压缩：在内存表读锁内仅做一次克隆（短暂持锁），随后在锁外把全量状态写入
  `snapshot.tmp` 并 fsync，再原子 rename 成 `snapshot.bin`；最后短暂进入提交临界区
  轮转 WAL、删除"完全被快照覆盖"的段。段可能跨越快照边界，恢复时这类记录按 LSN
  **跳过**而不是重复应用。
- 恢复：载入快照（基线 LSN）→ 按段顺序读 WAL：严格单调（重复/乱序即损坏）、
  LSN ≤ 快照位置的跳过、其后的必须连续无空洞地重放。

---

## 并发模型

- 读：`parking_lot::RwLock` 读锁，多读者互不阻塞；一次扫描在一把读锁内完成，
  因而看到的是某个已提交时刻的一致快照，读不到未提交中间态。
- 写/事务：tokio 异步互斥锁串行化"前置条件判断 → WAL fsync → 内存发布"。
  条件判断与发布之间不可能插入其他提交，CAS 在并发下保持比较与写入的原子性。
  阻塞的文件 I/O 通过 `spawn_blocking` 在专用线程执行。
- 版本：LSN 从 1 开始，每次已提交事务 +1；键的 `version` 即最后修改它的 LSN。

---

## 测试

```bash
cargo test
```

覆盖（共 32 个测试）：

- `tests/txn_all_or_nothing.rs` — 多键事务全有或全无、冲突回滚、同键矛盾操作/空事务/超限被拒
- `tests/concurrency_isolation.rs` — 8 任务并发 CAS 计数线性一致、并发大事务下扫描读不到
  撕裂的中间态、不相交事务互不干扰
- `tests/versions_and_cas.rs` — 版本严格单调（含删除后重建）、CAS 匹配/不匹配/空值语义、
  读取位置随提交推进
- `tests/recovery.rs` — 重启重放 WAL、快照 + 尾日志恢复、压缩后不重复应用、跨快照版本连续、
  torn 尾帧截断、压缩期间写入在重启后不丢
- `tests/config_and_scan.rs` — 非法配置启动期拒绝、键值大小限制、前缀/区间查询
- `tests/http_api.rs` — 经真实 axum 路由器的端到端接口与结构化错误

手动验证压缩与恢复：

```bash
curl -s -X POST 'localhost:8080/v1/compact?wait=true'   # 立即快照
# kill -9 服务进程后重新启动，检查 /v1/status 与数据
```

---

## 目录结构

```
src/
  main.rs       启动入口（配置校验 -> 恢复 -> 可选 seed -> HTTP 服务）
  config.rs     环境变量配置与启动期校验
  error.rs      结构化错误类型
  frame.rs      WAL/快照的 CRC 分帧读写
  record.rs     磁盘记录类型
  wal.rs        分段 WAL（追加/轮转/扫描/torn 尾处理/段回收）
  snapshot.rs   快照写入/加载/重放
  store.rs      内存表、事务、CAS、版本、压缩调度
  metrics.rs    Prometheus 指标
  api.rs        axum HTTP 接口
  seed.rs       预置算例
tests/          集成/端到端测试
docs/API.md     接口细节
scripts/demo.sh 一键演示
```
