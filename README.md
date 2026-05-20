# HookRelay

**Self-hosted webhook delivery infrastructure. Built for reliability, not hope.**

[![CI](https://github.com/rezilobz/hook-relay/actions/workflows/ci.yml/badge.svg)](https://github.com/rezilobz/hook-relay/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/rezilobz/hook-relay/branch/main/graph/badge.svg)](https://codecov.io/gh/rezilobz/hook-relay)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Backbone-Kafka-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![FastAPI](https://img.shields.io/badge/API-FastAPI-009688?logo=fastapi)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Deploy-Docker-2496ED?logo=docker)](https://www.docker.com/)

---

## What is HookRelay?

HookRelay is a self-hosted webhook delivery engine that takes the reliability problem seriously. When your application emits an event, HookRelay guarantees it reaches your customers' endpoints — with structured retry logic, dead letter queuing, full delivery history, and first-class observability — without requiring you to build or maintain that infrastructure yourself.

Most webhook implementations are afterthoughts: a `requests.post()` call inside a background task, a retry loop bolted on when the first customer complains. HookRelay is what that system should have been from the beginning.

---

## The Problem

Delivering webhooks reliably is deceptively hard:

- **Customer endpoints go down.** Your system needs to retry intelligently without hammering a recovering service.
- **Events must not be lost.** A crashed worker between receipt and delivery silently drops data.
- **At-least-once delivery must be idempotent.** Retries mean duplicates — consumers need to be able to detect them.
- **You need visibility.** When a customer says "we never got that event," you need a full delivery audit trail.
- **Scale matters.** A naive implementation that works at 10 events/second breaks badly at 10,000.

HookRelay solves these problems with a Kafka-backed architecture purpose-built for durable, observable, async event delivery.

---

## Architecture

```
                        ┌─────────────────────────────────────────────────────┐
                        │                    HookRelay                        │
                        │                                                     │
  Your Application      │  ┌─────────────┐     ┌───────────────────────────┐  │
        │               │  │             │     │                           │  │
        │  POST /events │  │   FastAPI   │     │     Kafka Topics          │  │
        └──────────────►│  │  Ingestion  ├────►│                           │  │
                        │  │   API       │     │  hookrelay.events.pending │  │
                        │  └─────────────┘     │  hookrelay.events.dlq     │  │
                        │                      │                           │  │
                        │  ┌─────────────┐     └──────────┬────────────────┘  │
                        │  │             │                │                   │
                        │  │  Control    │     ┌──────────▼───────────────┐   │
                        │  │  Plane API  │     │                          │   │
                        │  │  (FastAPI)  │     │   Delivery Worker Pool   │   │
                        │  │             │     │                          │   │
                        │  │  /endpoints │     │  ┌────────────────────┐  │   │
                        │  │  /events    │     │  │  Retry Scheduler   │  │   │
                        │  │  /deliveries│     │  │  (Redis ZSET)      │  │   │
                        │  │             │     │  └────────────────────┘  │   │
                        │  └──────┬──────┘     │                          │   │
                        │         │            │  ┌────────────────────┐  │   │
                        │         │            │  │  DLQ Handler       │  │   │
                        │  ┌──────▼──────┐     │  └────────────────────┘  │   │
                        │  │             │     │                          │   │
                        │  │ PostgreSQL  │◄────┤  Delivery Attempt Log    │   │
                        │  │             │     │  Idempotency Check       │   │
                        │  │  endpoints  │     │                          │   │
                        │  │  events     │     └──────────┬───────────────┘   │
                        │  │  deliveries │                │                   │
                        │  │  dlq        │                │  HTTPS POST       │
                        │  │             │                ▼                   │
                        │  └─────────────┘     Customer Endpoint              │
                        │                                                     │
                        │  ┌─────────────┐  ┌─────────────┐                   │
                        │  │  Prometheus │  │    Redis    │  retry ZSET       │
                        │  │  + Grafana  │  │    (AOF)    │  scheduler        │
                        │  │             │  │             │                   │
                        │  └─────────────┘  └─────────────┘                   │
                        └─────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Role |
|---|---|
| **Ingestion API** | Accepts incoming events from your application, validates payload, publishes to Kafka `events.pending` topic. Responds immediately — no synchronous delivery. |
| **Control Plane API** | CRUD for endpoint registration (URL, secret, event filters, enabled/disabled). Delivery history queries. Manual retry triggers. |
| **Kafka** | Durable event backbone. Two topics: `pending` for fresh events, `dlq` for exhausted deliveries. Retry scheduling is handled by Redis, not a dedicated Kafka topic. |
| **Delivery Worker Pool** | Consumes from Kafka, attempts HTTPS delivery to registered endpoints, writes attempt result to PostgreSQL, routes to retry or DLQ on failure. |
| **Retry Scheduler** | Coroutine that polls a Redis ZSET for due retries and republishes to `hookrelay.events.pending`. Computes next attempt time using exponential backoff with jitter. |
| **Redis** | Stores scheduled retry entries as a sorted set (score = `retry_after` Unix timestamp). AOF persistence must be enabled — a Redis restart without AOF would silently discard all pending retries. |
| **PostgreSQL** | Source of truth for endpoint configuration, event metadata, and full delivery attempt history. |
| **Prometheus + Grafana** | Tracks delivery success rate, retry depth distribution, Kafka consumer group lag, DLQ entry rate, and per-endpoint failure rates. |

---

## Core Features

- **At-least-once delivery** — Events are never dropped. Kafka consumer offsets are committed only after a successful delivery attempt write to PostgreSQL.
- **Exponential backoff with jitter** — Retries follow `min(cap, base * 2^attempt) + random jitter`. Starts at seconds, transitions to minutes, plateaus at 4-hour intervals — ~8.5 hours total window across 15 attempts.
- **Dead letter queue** — Events exhausting all retry attempts move to the DLQ. They are inspectable, replayable, and never silently discarded. Permanent client errors (4xx, except 429) bypass the retry queue entirely and go straight to the DLQ on first failure.
- **Idempotency keys** — Each event carries a unique ID. Workers check for prior successful delivery before attempting, making duplicate processing safe.
- **HMAC-SHA256 signatures** — Every delivery includes a signature header computed from the endpoint's secret. Consumers can verify authenticity.
- **Per-endpoint filtering** — Endpoints subscribe to specific event types. Unsubscribed events are never routed to them.
- **Full delivery audit trail** — Every attempt (success, failure, timeout) is logged with timestamp, HTTP status, response body (truncated), and latency.
- **Prometheus metrics** — First-class observability, not an afterthought. Ships with a Grafana dashboard definition.
- **Docker Compose** — Full local stack (API, workers, Kafka, PostgreSQL, Prometheus, Grafana) in a single command.

---

## Quickstart

**Prerequisites:** Python 3.12+, [UV](https://docs.astral.sh/uv/), Docker, Docker Compose.

```bash
git clone https://github.com/rezilobz/hook-relay.git
cd hook-relay
cp .env.example .env
make install-dev
make up
```

The control plane API is available at `http://localhost:8000`. Grafana at `http://localhost:3000`.

**Register an endpoint:**
```bash
curl -X POST http://localhost:8000/endpoints \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://your-service.example.com/webhooks",
    "description": "Production order events",
    "event_types": ["order.created", "order.cancelled"],
    "secret": "your-signing-secret"
  }'
```

**Send an event:**
```bash
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "event_type": "order.created",
    "idempotency_key": "order-8821-created",
    "payload": {
      "order_id": "8821",
      "customer_id": "u-4491",
      "total": 149.99
    }
  }'
```

**Check delivery status:**
```bash
curl http://localhost:8000/events/{event_id}/deliveries
```

---

## API Reference

### Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/endpoints` | Register a new webhook endpoint |
| `GET` | `/endpoints` | List all registered endpoints |
| `GET` | `/endpoints/{id}` | Get endpoint details and delivery stats |
| `PATCH` | `/endpoints/{id}` | Update endpoint URL, secret, or enabled state |
| `DELETE` | `/endpoints/{id}` | Deregister endpoint |

### Events

| Method | Path | Description |
|---|---|---|
| `POST` | `/events` | Ingest a new event for delivery |
| `GET` | `/events/{id}` | Get event metadata and delivery status |
| `GET` | `/events/{id}/deliveries` | Full delivery attempt history for an event |
| `POST` | `/events/{id}/retry` | Manually re-queue an event (including from DLQ) |

### Dead Letter Queue

| Method | Path | Description |
|---|---|---|
| `GET` | `/dlq` | List all DLQ entries with filter options |
| `POST` | `/dlq/{id}/replay` | Re-queue a DLQ entry for delivery |
| `DELETE` | `/dlq/{id}` | Discard a DLQ entry permanently |

---

## Design Decisions

These are the non-obvious choices made during design, and the reasoning behind each. This section exists because the tradeoffs matter more than the decisions.

### Why Kafka, not Redis Streams or a database queue?

The primary contenders were Kafka, Redis Streams, and a PostgreSQL-backed queue (e.g. with `pg_notify` or a simple polling loop).

A PostgreSQL queue was rejected because polling introduces latency, and high-throughput event ingestion against a transactional database creates write pressure that conflicts with the read patterns of the delivery history queries. They're different workloads that deserve different storage.

Redis Streams was a serious candidate. It's operationally simpler than Kafka, supports consumer groups, and has sub-millisecond latency. For a deployment at moderate scale (< ~5,000 events/second), Redis Streams is arguably the better choice. The reason Kafka was chosen here is deliberate: even with Redis handling retry scheduling, Kafka's durable log makes it possible to replay the entire event history if a worker bug causes incorrect delivery processing — something Redis Streams cannot do after messages are consumed.

The honest answer is: if you're self-hosting HookRelay for a team of 10, Redis Streams is probably the right call. Kafka is chosen here to handle the scale tier where HookRelay is actually worth deploying over a managed alternative.

### Why exponential backoff with jitter instead of fixed intervals?

Fixed retry intervals create synchronized retry storms. If 500 endpoints fail simultaneously (e.g. during a brief network partition), fixed intervals mean 500 workers retry at exactly the same moments. Jitter distributes those retries across the backoff window, smoothing load on both HookRelay's worker pool and the customer's recovering endpoint.

The formula used is `min(cap, base * 2^n) + uniform_random(0, base)` where `base = 1s` and `cap = 4 hours`. This gives retry intervals that start fast (2s, 4s, 8s, 16s…), transition through minutes (~1min, ~2min, ~4min, ~8min, ~17min, ~34min), and eventually plateau at 4-hour intervals. With `MAX_RETRY_ATTEMPTS = 15`, the total retry window is approximately 8.5 hours before a delivery is moved to the DLQ.

### Why commit Kafka offsets after PostgreSQL write, not after delivery?

Committing after HTTP delivery would mean a worker crash between delivery and commit causes the event to be re-delivered on worker restart — which is fine if the consumer is idempotent, but double delivery is still an unnecessary hazard. Committing after the attempt is written to PostgreSQL means re-delivery can be detected by idempotency key check before the HTTP call is made, making duplicates a handled edge case rather than a silent one.

### Why HMAC-SHA256 over API keys for delivery verification?

API keys in webhook headers can be logged by intermediaries (load balancers, proxies, monitoring tools). HMAC signatures are computed per-payload — even if the signature is logged, it cannot be reused against a different payload. This is the same approach used by Stripe, GitHub, and Shopify for the same reason.

### Why PostgreSQL for delivery state, not Kafka itself?

Kafka is not a database. Using it as one — querying delivery history by endpoint, filtering attempts by status, computing per-endpoint failure rates — would require either maintaining a consumer that projects Kafka events into a queryable store (which is just a database with extra steps) or abusing consumer group offset tracking as a query mechanism. PostgreSQL is the right tool for structured, queryable state. Kafka is the right tool for durable, ordered event transport. HookRelay uses both for what they're each good at.

### Why resolve endpoint fan-out at consumption, not at ingestion?

When an event arrives, it may match multiple registered endpoints. The fan-out could happen at two points: immediately at ingestion (publish one Kafka message per matching endpoint) or lazily at consumption (publish one message per event, let the worker resolve matching endpoints).

Fan-out at ingestion keeps the worker simple but couples the ingestion API to a database query on every request — which breaks the intent of responding immediately with no synchronous work. It also means endpoints registered after an event was published can never receive it, even if the event is replayed. Fan-out at consumption keeps ingestion pure: receive event, publish one message, return. The worker resolves current matching endpoints at processing time, which means replays and late-registered endpoints work correctly. The `DeliveryAttempt` table — one row per `(event, endpoint, attempt)` — is the natural representation of this model.

### Why asyncio tasks instead of a thread pool for delivery workers?

A common misconception: Python's GIL prevents threads from running Python code in parallel, but it is released during I/O operations. This means thread pools *can* achieve real concurrency for I/O-bound work like HTTP delivery — a thread waiting on a network response is not holding the GIL. So both approaches work. Asyncio wins for different reasons: it is consistent with the rest of the stack (`aiokafka`, `httpx`, and SQLAlchemy async are all async-native), coroutines have far lower memory overhead than threads (~KB vs ~8MB stack per unit), cooperative switching at `await` points is cheaper than OS-level context switching, and there are no shared-state concurrency bugs to reason about. `WORKER_CONCURRENCY` controls the number of concurrent asyncio delivery tasks, not threads.

### Why check the idempotency key at ingestion, not only at delivery?

Idempotency is enforced at two layers. At ingestion, a `UNIQUE` constraint on `idempotency_key` combined with `INSERT ... ON CONFLICT DO NOTHING` rejects duplicate events before they enter Kafka — a clean system state with no redundant messages. At delivery, the worker checks for an existing successful `DeliveryAttempt` record before making the HTTP call, which handles the case where a worker crashes after delivery but before committing the Kafka offset and the event is re-processed on restart.

The ingestion check adds roughly 1–3ms of latency (one indexed DB roundtrip). This is acceptable: the ingestion endpoint is already an HTTP call with network round-trip latency, and allowing duplicates into Kafka is not free either — they burn worker capacity, Kafka storage, and produce spurious delivery records.

### Why is the HMAC signature computed at delivery time, not stored at ingestion?

The HMAC-SHA256 signature sent in `X-HookRelay-Signature` is computed as `HMAC(payload, endpoint.secret)`. If the signature were pre-computed at ingestion and stored, rotating an endpoint's secret would immediately invalidate all stored signatures for un-delivered events — replayed or retried events would arrive with a signature the consumer's new secret cannot verify. Computing the signature fresh at delivery time means the current secret is always used, regardless of when the event was originally ingested. Secret rotation works correctly with no special handling.

### Why is event status both stored and derived?

The `Event` table carries a `status` column (`pending` / `retrying` / `delivered` / `partially_retrying` / `partially_delivered` / `dlq`) for fast list queries. Without it, `GET /events` would require joining against `DeliveryAttempt` and aggregating per event — expensive at scale. However, a stored field can drift out of sync if a worker crashes mid-update. The resolution: the stored `status` is a read cache updated asynchronously by workers, but the source of truth is always the `DeliveryAttempt` records. `GET /events/{id}` (single event detail) derives status fresh from attempts. `GET /events` (list) uses the stored field. Inconsistencies are transient and self-correcting as workers complete.

### Why Redis for retry scheduling, not a dedicated Kafka retry topic?

The original design used a `hookrelay.events.retry` Kafka topic with a delay mechanism for scheduled re-attempts. Redis was introduced instead because it provides a first-class primitive for this pattern: a sorted set (ZSET) where the score is the `retry_after` Unix timestamp. A scheduler coroutine polls for due retries with an atomic Lua script (`ZRANGEBYSCORE` + `HGET`/`HDEL` + `ZREM`), then re-publishes to `hookrelay.events.pending`. The ZSET holds scores (retry timestamps); a companion hash holds the full Kafka message payload keyed by `{event_id}:{endpoint_id}`, allowing O(1) cancellation and arbitrary message data without encoding it into the ZSET member. This is strictly simpler than implementing delayed delivery semantics on top of Kafka, which has no native delay support.

Redis also unlocks per-endpoint rate limiting and circuit breaker state in v0.2 without adding a new operational dependency at that point — the service is already running.

**Durability caveat:** Redis is in-memory by default. AOF (append-only file) persistence must be enabled (`--appendonly yes`) so that a Redis restart does not silently discard all scheduled retries. In Docker Compose this is passed as a command argument to the Redis service. In production, treat Redis persistence configuration as a reliability requirement, not an optional tuning parameter.

**Scheduler delivery semantics:** The poll-and-republish flow is two-phase, not atomic. A Lua script fetches due entries from the ZSET (`ZRANGEBYSCORE` + `HGET`) without removing them. Each entry is published to `hookrelay.events.pending`, then removed from Redis via `cancel()` (`ZREM` + `HDEL`). A crash between publish and cancel leaves the entry in Redis; it will be re-fetched on the next poll and produce a duplicate Kafka message, which the delivery worker's idempotency check suppresses. This gives at-least-once delivery with no message loss.

### Why publish to Kafka before committing, not after?

Publishing before commit means a Kafka failure rolls back the DB transaction — the event is never persisted without a corresponding worker signal. The client gets a 500 and can safely retry with the same idempotency key, which will be treated as a fresh insert.

The reverse failure mode — DB commit failing after a successful Kafka publish — produces an orphan message for an `event_id` that was never committed. The worker queries the DB for that `event_id`, finds nothing, and skips the message. The client sees a 500 and retries; the idempotency key insert lands cleanly since the first transaction rolled back.

This trade-off is deliberate: DB commit failures are orders of magnitude rarer than Kafka failures, and both failure modes have safe recovery paths. The alternative — an outbox table committed in the same transaction, with a background publisher retrying — eliminates the dual-write gap entirely but adds a polling process, distributed locking (`FOR UPDATE SKIP LOCKED`), and a new table. That operational complexity is not justified given the rarity of the failure and the acceptable recovery behaviour.

### Why does Alembic use the async engine instead of a separate psycopg2 engine?

The traditional Alembic setup uses a synchronous `psycopg2` engine for migrations and a separate `asyncpg` engine for the running application. This avoids asyncio complexity in migration scripts at the cost of maintaining two DB drivers as dependencies.

HookRelay uses a single driver (`asyncpg`) for both. Alembic's async migration support (`async_engine_from_config` + `asyncio.run()`) has been stable since Alembic 1.x and is the approach recommended in the official Alembic async documentation. The `migrations/env.py` constructs its own engine from the Alembic config at migration time, independent of the application engine in `db/engine.py`. No psycopg2 dependency, no dual-driver maintenance.

### Why are delivery workers a separate process from the API?

The ingestion API and the delivery workers have different scaling profiles: you might run two API replicas behind a load balancer while running ten worker instances to increase delivery throughput. If they shared a process, they could only scale together. Running them as separate OS processes — the API via `uvicorn`, the workers via a dedicated `hookrelay-worker` entry point — also means a worker crash or Kafka consumer rebalance does not affect API availability, and vice versa.

### Why do 4xx errors skip retries and go straight to the DLQ?

Retry backoff is designed to handle *transient* failures — a momentarily overloaded server, a brief network interruption, a deploying service. A 4xx response is not transient: it indicates a permanent problem with the *request itself* — a misconfigured URL (404), a disallowed method (405), an authentication failure (401/403). Retrying the exact same request against the same endpoint will produce the exact same error every time, burning 15 retry slots and up to 8.5 hours of wall time with no chance of recovery.

The one exception is **429 Too Many Requests**, which is explicitly transient — the server is functioning correctly and asking you to slow down. HookRelay treats 429 as a normal retryable failure.

All other 4xx responses go immediately to the DLQ on first failure, where they are visible, inspectable, and replayable once the endpoint configuration is corrected.

### Why random Kafka partitioning (no ordering guarantee per endpoint)?

Partitioning Kafka messages by `endpoint_id` would ensure all events for a given endpoint land in the same partition and are processed sequentially — guaranteeing delivery order. The tradeoff is head-of-line blocking: if one endpoint is slow or down, its entire partition stalls while other endpoints wait. Random (round-robin) partitioning maximises worker parallelism at the cost of ordering guarantees. For most webhook use cases, strict delivery ordering is not a requirement — consumers should be idempotent regardless. Ordered delivery per endpoint is a candidate feature for a future version, implementable by adding a partition-by-`endpoint_id` mode alongside a sequential delivery option in the worker.

---

## Configuration

HookRelay is configured via environment variables. See `.env.example` for all options.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `DATABASE_URL` | `postgresql+asyncpg://hookrelay:hookrelay@localhost:5432/hookrelay` | PostgreSQL connection string (asyncpg driver required) |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string. AOF persistence must be enabled — a restart without AOF silently discards all scheduled retries |
| `API_KEY` | *(required)* | Secret used to authenticate requests to the ingestion and control plane APIs |
| `WORKER_CONCURRENCY` | `10` | Maximum concurrent in-flight HTTP delivery tasks |
| `MAX_RETRY_ATTEMPTS` | `15` | Attempts before DLQ (~8.5h total window: 2s → 4s → … → 4h) |
| `DELIVERY_TIMEOUT_SECONDS` | `10` | Per-attempt HTTP timeout |
| `SIGNATURE_HEADER` | `X-HookRelay-Signature` | Header name for HMAC payload signature |
| `API_KEY_HEADER` | `X-API-Key` | Header name for ingestion API authentication |

---

## Observability

HookRelay exposes a `/metrics` endpoint in Prometheus format. Key metrics:

| Metric | Type | Description |
|---|---|---|
| `hookrelay_events_ingested_total` | Counter | Total events accepted by ingestion API |
| `hookrelay_deliveries_attempted_total` | Counter | Total delivery attempts, labelled by outcome |
| `hookrelay_delivery_latency_seconds` | Histogram | HTTP delivery round-trip latency |
| `hookrelay_retry_depth` | Histogram | Distribution of attempt number at time of success |
| `hookrelay_dlq_entries_total` | Gauge | Current DLQ depth |
| `hookrelay_kafka_consumer_lag` | Gauge | Consumer group lag per topic partition |

A Grafana dashboard definition is included at `infra/grafana/dashboard.json`.

---

## Roadmap

### v0.1 — Core delivery engine *(current focus)*
- [x] Event ingestion API
- [x] Endpoint registration and management
- [x] Kafka-backed delivery worker
- [x] Exponential backoff retry logic
- [x] Dead letter queue
- [x] Delivery attempt history
- [x] HMAC-SHA256 payload signing
- [x] Docker Compose local stack
- [x] Prometheus metrics

### v0.2 — Operational hardening
- [ ] Retry deduplication token (prevent duplicate Kafka messages on concurrent manual retries)
- [ ] Per-endpoint rate limiting (protect slow consumers)
- [ ] Circuit breaker per endpoint (auto-disable consistently failing endpoints)
- [ ] Bulk event ingestion endpoint
- [ ] Endpoint health check and auto-recovery
- [ ] Index PostgreSQL tables for common queries

### v0.3 — Developer experience
- [ ] Web UI for delivery monitoring and DLQ management
- [ ] CLI tool for local testing (`hookrelay send --event order.created --payload '{}'`)
- [ ] Delivery log export (CSV / JSON)
- [ ] Event schema validation support

### v0.4 — Scale and deployment
- [ ] Kubernetes Helm chart
- [ ] Horizontal worker scaling documentation
- [ ] Standalone retry scheduler process (`hookrelay-scheduler`) — currently the retry scheduler runs as a coroutine inside each worker replica, which is correct (duplicate Kafka messages from concurrent replicas are suppressed by the delivery worker's idempotency check) but results in redundant Redis polling and extra Kafka traffic proportional to replica count. At high replica counts, extract the scheduler into its own single-replica deployment with a separate entry point; the interface already supports this (`run_scheduler` is a standalone coroutine)
- [ ] Multi-tenancy support (isolated endpoint namespaces per API key)
- [ ] Redis-backed alternative transport layer for lightweight deployments

---

## Contributing

Contributions are welcome. For significant changes, please open an issue first to discuss scope and approach. All PRs require passing tests and updated documentation.

```bash
make install-dev   # install deps + pre-commit hooks
make test          # run unit tests
make test-cov      # run tests with coverage report
make check         # lint + format check + type check
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide.

---

## License

MIT. See [LICENSE](LICENSE) for details.
