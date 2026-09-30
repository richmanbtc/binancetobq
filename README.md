# Go collector

A Go collector for public market candles, with incremental aggregation and
BigQuery uploads. The repository builds and publishes the container image;
configuration, credentials and process supervision belong to the deployment.

## Design

- A WebSocket reader forwards closed one-minute candles and the first open
  update per minute and symbol to a bounded queue. Open events carry only a
  timestamp; their prices never enter aggregation. Repeated open updates are
  discarded.
- WebSocket finality uses the exchange's `k.x` flag. REST finality requires a
  later candle in the same response, never the collector's wall clock. Catch-up
  omits `endTime` and withholds the newest row. Pagination re-fetches the
  previous page's tail so a formerly open candle is consumed with fresh values
  once its successor appears. A symbol with no later candle can retain an
  unprocessed tail.
- Live gap requests include the boundary candle as evidence but aggregate only
  earlier rows. An unproven remaining tail stops collection for external restart
  instead of advancing past it. Missing source minutes are not synthesized.
- Acquisition emits verified half-open ranges, including proven missing minutes.
  `aggregate.Engine.Advance` checks contiguity, ordering and candle membership
  before mutation. The range watermark also closes buckets with missing final
  minutes; no empty buckets or synthetic prices are emitted.
- One goroutine owns incremental aggregation with bounded per-symbol state,
  without DataFrames, locks or an aggregation worker pool.
- A separate writer batches completed rows into BigQuery JSON load jobs (see
  Compatibility for flush and retry behavior).
- Initial history is recovered from the oldest unfinished interval per symbol.
  After connecting, each symbol waits for its first WebSocket candle before
  requesting REST history again. REST must reach that candle's timestamp before
  the held candle can advance aggregation; an open REST tail also proves
  coverage. A missing minute inside that covered range is treated as absent at
  the source. Empty or lagging REST responses get three publication retries (1,
  2, and 4 seconds of backoff). Continued lack of overlap ends collection for
  external restart, including during live gap repair. This assumes REST pages
  are complete through their last returned timestamp. Socket candles queue
  during REST requests; duplicates are ignored. A new open candle can trigger
  REST repair of a missed close, even when the socket remains connected and only
  open updates are arriving.
- WebSocket connection failures and disconnects end collection immediately. Each
  restart re-reads warehouse checkpoints and recovers unfinished intervals,
  including discarded queues.
- Invalid-symbol errors exclude that symbol. Transport failures, HTTP 418/429
  and server errors receive up to eight attempts with bounded exponential
  delays; Retry-After is honored. Other HTTP errors and malformed responses stop
  collection. For an HTTP date, the response Date header is used to avoid local
  clock skew when available.
- A full receive or save queue ends collection for external restart and
  recovery.
- Initial REST history bypasses the receive queue; completed aggregates enter
  the save queue. The receive queue holds 4,096 candle events. The save queue
  holds 65,536 rows, budgeting approximately 16 MiB at 256 bytes per row
  including referenced values and headroom. In-flight batches, JSON, and client
  buffers are additional memory, so this is not a process memory limit.
- A WebSocket read times out after 90 seconds without a data message. Open
  candles count as messages. Individual symbols are not monitored for progress;
  the once-per-minute running log reports counts, not end-to-end health.
- A single watchdog starts at the first statement of main, before configuration
  or client setup. Only successful nonempty BigQuery appends reset its timer.
  Receiving data, processing REST pages, empty flushes, and retries never reset
  it. Missing upstream data also triggers the deadline; worker activity cannot
  extend it.
- The caller passes a no-save timeout of the shortest configured interval plus
  15 minutes: 20 minutes with five-minute output, or 75 minutes with hourly
  output only. Before configuration is read, the longest supported interval is
  assumed. The deadline runs from process start or the last successful append,
  with no startup grace. Uploads have a separate 10-minute cooperative timeout.
  The shared watchdog tracks only timeout and pings, not individual symbols or
  table freshness.
- The watchdog checks monotonic elapsed time every second and exits with code 2
  without logging, cleanup, or application locks. Detection can take one extra
  polling second. Long Retry-After waits do not extend the no-save limit;
  restart can therefore occur before a server-requested wait finishes. The
  external restart policy should include backoff. A process-wide suspension
  still needs external supervision because an internal watchdog cannot execute
  then.
- There is no graceful shutdown or shutdown budget. SIGINT/SIGTERM use the OS
  default termination behavior. Errors return without draining queues or waiting
  for workers or client cleanup. Uncommitted rows are recovered on restart.
- Logs include symbols, recovery ranges and durations, save row and symbol
  counts, saved timestamp ranges, and retry status, attempts and waits. Errors
  identify the failing stage. Endpoints, credentials and project/dataset
  identifiers are omitted. DEBUG also reports historical page details. A failed
  writer stops collection. Timestamp ranges use UTC; processed_until is an
  exclusive boundary, while saved ranges describe bucket start times. Retry
  status 0 means no HTTP status was available.

## Package boundaries

All implementation packages are internal. Project-local imports are limited to:

| Package | Responsibility | Project dependencies |
| --- | --- | --- |
| `model` | Shared data, validation and the invalid-symbol error | None |
| `config` | Environment parsing, defaults and save deadline policy | None |
| `retry` | Cancellation-aware waiting and bounded backoff | None |
| `watchdog` | Timeout and ping tracking | None |
| `aggregate` | Aggregation state and resume positions | `model` |
| `exchange` | REST history and WebSocket events | `model`, `retry` |
| `warehouse` | BigQuery preparation, checkpoints and appends | `model`, `retry` |
| `collector` | Recovery, live handoff and gap repair | `model` |
| `writer` | Periodic queue snapshots and save notifications | `model` |

The root `main` package constructs and connects implementations. It passes
`exchange.Client` and `aggregate.Engine` through the consumer-owned
`collector.Source` and `collector.Aggregator` interfaces. The writer receives a
save function and a success callback, without importing warehouse or watchdog.
Configuration is translated into each implementation's own options at the root.
SDK and connection types stay inside their implementation packages.

The longest project-local import chain has two edges: main to an implementation,
then to model or retry. The architecture test enforces the dependencies above.
Package tests cover private state and collector gap/handoff behavior directly.
Root integration tests combine public APIs and exercise concurrent sessions.
Replay uses the same Resume/Advance aggregation entry points.

## Build and verify

Use Go 1.26 or later, from the repository root:

```sh
go test -race ./...
go vet ./...
go test -run '^$' -bench . -benchmem ./...
go build -trimpath -buildvcs=false -o collector .
```

Tests do not access production services or credentials. They include synthetic
Python golden results, duplicate handling, interval boundaries, recovery
offsets, REST pagination, rate limits, malformed messages, missed close
recovery, buffered per-symbol handoff, ambiguous upload retries, verified empty
ranges, and immediate exit without waiting for a noncooperative writer.
Virtual-time tests check save-only pings, uniform interval-based deadlines, and
empty queues. Subprocess tests verify actual hard exits for stalled HTTP
transports, response bodies, cleanup, stores, logs, plain blocking code, and
excessive Retry-After waits. `go test ./internal/exchange -run '^$' -fuzz
FuzzExchangeDecoders -fuzztime 60s -parallel 2` exercises both transport
decoders with synthetic inputs.

`testdata/python_expected.json` is a frozen compatibility fixture generated from
the former Python `_rows_to_df` and `_process_df` implementations using
synthetic candles. Keep it independent of the Go implementation. Its generator
is not included; Go tests need no Python dependencies.

## Development container

The development container builds `.devcontainer/Dockerfile` locally from the
Docker Official Go image on Debian. It installs Git, jq, procps, ripgrep, tmux
and the standalone Codex CLI from its official release, without editor
extensions or Node.js. The root `config.toml` is mounted read-only at Codex's
user configuration path; it is never copied into either image. Authenticate
Codex inside the container. Rebuilding the development image installs the latest
Codex release. The collector image uses the separate root Dockerfile.

## Offline comparison

```sh
go run . -replay < candles.jsonl > aggregates.jsonl
```

Each input line is a JSON object containing a **closed** one-minute candle:
`Symbol`, `Time` (UTC Unix seconds), `Open`, `High`, `Low`, `Close`, `Volume`,
`Amount`, `Trades`, `BuyVolume`, and `BuyAmount`. Input must be chronological
per symbol; different symbols can be interleaved. Repeated/older timestamps are
ignored. Output contains `Interval` (seconds) and `Row` (warehouse column
names). Replay uses both supported intervals, performs no network access, and
does not require runtime configuration. The last incomplete bucket is not
emitted.

## Runtime configuration

Supply these environment variables externally; do not commit actual values.

| Variable | Meaning |
| --- | --- |
| `GC_PROJECT_ID` | BigQuery project ID; legacy domain-scoped IDs are accepted |
| `BINANCETOBQ_DATASET` | Existing `dataset_id` or `project_id.dataset_id`; dataset ID uses 1-1024 letters, digits or underscores |
| `BINANCETOBQ_SYMBOLS` | Comma-separated base assets without USDT; USDT is appended; at most 200 unique symbols per process |
| `BINANCETOBQ_MARKET_TYPE` | `spot` or `perp` (USD-M futures) |
| `BINANCETOBQ_INTERVALS` | `5m`, `1h`, or both comma-separated |
| `BINANCETOBQ_LOG_LEVEL` | `DEBUG`/`NOTSET`, `INFO`, `WARNING`/`WARN`, `ERROR`, `CRITICAL`/`FATAL` (case-insensitive); defaults to `INFO` |

An unqualified dataset belongs to `GC_PROJECT_ID`. A qualified dataset selects
its own destination project; `GC_PROJECT_ID` remains the project used to submit
and bill BigQuery jobs. The execution identity needs access to both projects.

BigQuery infers job locations from the destination or referenced dataset;
existing load-job recovery reads the dataset location automatically. REST and
WebSocket URLs are built in and selected by market type; futures klines use the
market stream route. Enabled intervals select these Python-compatible tables:

| Market | `5m` | `1h` |
| --- | --- | --- |
| `spot` | `binance_ohlcv_spot_5m` | `binance_ohlcv_spot` |
| `perp` | `binance_ohlcv_5m` | `binance_ohlcv` |

Without a table or symbol checkpoint, history starts at Unix time zero and
recovers all available candles. Otherwise collection resumes after that
interval's last stored bucket. New tables can require large backfills. Existing
history remains untouched; startup creates missing tables, which must exist
before loading. The dataset must already exist.

Before collecting data, startup checks each enabled table for a nullable
`ingested_at TIMESTAMP` column with `DEFAULT CURRENT_TIMESTAMP()`. Existing
tables are migrated by adding the column, then setting its default in a separate
SQL statement. Startup is repeatable after partial migrations; new tables
include the default. An incompatible column or schema preparation failure stops
startup.

The timestamp is assigned by BigQuery immediately before writing during job
processing, not by the collector and not at job completion. The JSON payload and
load input schema intentionally omit this column. Existing rows remain NULL when
the column is added; setting a default does not backfill historical upload
times.

Use Google Application Default Credentials supplied by the execution
environment. No exchange API key is needed for public market data. Do not put
credentials in the image. The identity needs query, job, and destination-table
access, including table metadata read, table creation when absent, and schema
update permission for automatic migration. The dataset itself is not changed.

With configuration and authentication supplied, run:

```sh
./collector
```

Deployment must restart failed processes with backoff; the collector does not
reconnect WebSockets. REST rate-limit waits and load-job retries run inside the
process, subject to the save watchdog. No shutdown grace is required.

## Compatibility and intentional differences

- Column names and formulas match Python, including sample standard deviations
  (`ddof=1`), singleton standard deviations of zero, and hourly `twap_5m` as the
  mean of the last close in each five-minute sub-bucket.
- The legacy names `hi_op_max` and `lo_op_min` still contain **means**, not
  extrema. `cl_diff_std` includes the first close minus the first open, then
  consecutive close differences. These unusual definitions are intentionally
  preserved.
- Aggregates are finalized on the closing one-minute candle at the bucket end,
  rather than waiting for the next minute's first update. Only timing changes;
  the final values match. Floating-point roundoff may differ slightly.
- Like Python, an interval containing missing source minutes is aggregated from
  the available candles. If the exchange no longer provides those minutes, this
  collector cannot recreate them. Verified coverage through the bucket end
  closes such a partial interval even if no later closed candle is available.
- REST and stream timestamps are UTC. Daily/timezone-shifted streams are not
  used.
- The candle `timestamp` remains integer seconds and numeric measures remain
  FLOAT. The additional `ingested_at` column uses BigQuery TIMESTAMP.
- Writes take a snapshot of the save queue every five seconds when rows exist,
  with no row-count trigger or final flush. Uploads are sequential, so slow
  uploads delay the next flush. No separate unbounded buffer accumulates between
  flushes. Large historical backfills remain subject to exchange and BigQuery
  quotas.
- A load job retains one job ID across ambiguous transport retries. This avoids
  duplicate appends within that retry. It is **not** cross-process exactly-once
  delivery: a crash while a job is committing, concurrent writers, or external
  table modifications still require reconciliation. Only one writer per target
  market/table set should run.
- Checkpoints use MAX(timestamp), as in Python. They do not detect older
  interior holes. Repairing pre-existing holes is a separate, explicit
  operation.
- Raw transport error bodies are suppressed. Warehouse load failures log the
  submission, polling or job stage, HTTP status (0 when unavailable), recognized
  API reason and diagnostic clauses, including known-column type/mode changes.
  Free-form messages, unknown field names and error locations are omitted to
  avoid exposing credentials, configuration or input data. Unrecognized details
  still require checking the deployment and warehouse consoles.

## Container image

```sh
docker build -t collector .
```

The root Dockerfile tests and builds in a separate stage, then runs as an
unprivileged user without development tools. GitHub Actions tests and builds on
branch/tag pushes and manual runs; it publishes on pushes to the default branch,
`feature/*` branches and `v*` tags. The default branch uses `latest`; other
published refs use a sanitized ref name.

Supply runtime configuration, credentials and restart supervision externally.
Mounted credentials must be readable by the container user. Keep an older image
for rollback. Environment variable and table names retain the Python interface.

References:

- https://pkg.go.dev/cloud.google.com/go/bigquery
- https://pkg.go.dev/github.com/coder/websocket
- https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
-
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams
