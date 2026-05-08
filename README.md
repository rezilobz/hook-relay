# HookRelay

**Self-hosted webhook delivery infrastructure. Built for reliability, not hope.**

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
                        │  │             │     │  hookrelay.events.retry   │  │
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
                        │  │  /deliveries│     │  │  (exp. backoff)    │  │   │
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
                        │  ┌─────────────┐                                    │
                        │  │  Prometheus │  /metrics                          │
                        │  │  + Grafana  │◄── worker, Kafka consumer lag,     │
                        │  │             │    delivery success/fail rates     │
                        │  └─────────────┘                                    │
                        └─────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Role |
|---|---|
| **Ingestion API** | Accepts incoming events from your application, validates payload, publishes to Kafka `events.pending` topic. Responds immediately — no synchronous delivery. |
| **Control Plane API** | CRUD for endpoint registration (URL, secret, event filters, enabled/disabled). Delivery history queries. Manual retry triggers. |
| **Kafka** | Durable event backbone. Three topics: `pending` for fresh events, `retry` for scheduled re-attempts, `dlq` for exhausted deliveries. |
| **Delivery Worker Pool** | Consumes from Kafka, attempts HTTPS delivery to registered endpoints, writes attempt result to PostgreSQL, routes to retry or DLQ on failure. |
| **Retry Scheduler** | Computes next attempt time using exponential backoff with jitter, publishes to `retry` topic with appropriate delay. |
| **PostgreSQL** | Source of truth for endpoint configuration, event metadata, and full delivery attempt history. |
| **Prometheus + Grafana** | Tracks delivery success rate, retry depth distribution, Kafka consumer group lag, DLQ entry rate, and per-endpoint failure rates. |

---

## Core Features

- **At-least-once delivery** — Events are never dropped. Kafka consumer offsets are committed only after a successful delivery attempt write to PostgreSQL.
- **Exponential backoff with jitter** — Retries follow `min(cap, base * 2^attempt) + random jitter` to avoid thundering herd against recovering endpoints.
- **Dead letter queue** — Events exhausting all retry attempts move to the DLQ. They are inspectable, replayable, and never silently discarded.
- **Idempotency keys** — Each event carries a unique ID. Workers check for prior successful delivery before attempting, making duplicate processing safe.
- **HMAC-SHA256 signatures** — Every delivery includes a signature header computed from the endpoint's secret. Consumers can verify authenticity.
- **Per-endpoint filtering** — Endpoints subscribe to specific event types. Unsubscribed events are never routed to them.
- **Full delivery audit trail** — Every attempt (success, failure, timeout) is logged with timestamp, HTTP status, response body (truncated), and latency.
- **Prometheus metrics** — First-class observability, not an afterthought. Ships with a Grafana dashboard definition.
- **Docker Compose** — Full local stack (API, workers, Kafka, Zookeeper, PostgreSQL, Prometheus, Grafana) in a single command.

---

## Quickstart

```bash
git clone https://github.com/yourhandle/hookrelay.git
cd hookrelay
cp .env.example .env
docker compose up
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

Redis Streams was a serious candidate. It's operationally simpler than Kafka, supports consumer groups, and has sub-millisecond latency. For a deployment at moderate scale (< ~5,000 events/second), Redis Streams is arguably the better choice. The reason Kafka was chosen here is deliberate: the multi-topic routing pattern (pending → retry → dlq) maps cleanly to Kafka's topic semantics, and Kafka's durable log makes it possible to replay the entire event history if a worker bug causes incorrect delivery processing — something Redis cannot do after messages are consumed.

The honest answer is: if you're self-hosting HookRelay for a team of 10, Redis Streams is probably the right call. Kafka is chosen here to handle the scale tier where HookRelay is actually worth deploying over a managed alternative.

### Why exponential backoff with jitter instead of fixed intervals?

Fixed retry intervals create synchronized retry storms. If 500 endpoints fail simultaneously (e.g. during a brief network partition), fixed intervals mean 500 workers retry at exactly the same moments. Jitter distributes those retries across the backoff window, smoothing load on both HookRelay's worker pool and the customer's recovering endpoint.

The formula used is `min(cap, base * 2^n) + uniform_random(0, base)` where `base = 1s` and `cap = 30 minutes`. This gives retry intervals of approximately 1s, 2s, 4s, 8s, 16s, 32s, 64s... up to 30 minutes, with randomisation at each step.

### Why commit Kafka offsets after PostgreSQL write, not after delivery?

Committing after HTTP delivery would mean a worker crash between delivery and commit causes the event to be re-delivered on worker restart — which is fine if the consumer is idempotent, but double delivery is still an unnecessary hazard. Committing after the attempt is written to PostgreSQL means re-delivery can be detected by idempotency key check before the HTTP call is made, making duplicates a handled edge case rather than a silent one.

### Why HMAC-SHA256 over API keys for delivery verification?

API keys in webhook headers can be logged by intermediaries (load balancers, proxies, monitoring tools). HMAC signatures are computed per-payload — even if the signature is logged, it cannot be reused against a different payload. This is the same approach used by Stripe, GitHub, and Shopify for the same reason.

### Why PostgreSQL for delivery state, not Kafka itself?

Kafka is not a database. Using it as one — querying delivery history by endpoint, filtering attempts by status, computing per-endpoint failure rates — would require either maintaining a consumer that projects Kafka events into a queryable store (which is just a database with extra steps) or abusing consumer group offset tracking as a query mechanism. PostgreSQL is the right tool for structured, queryable state. Kafka is the right tool for durable, ordered event transport. HookRelay uses both for what they're each good at.

---

## Configuration

HookRelay is configured via environment variables. See `.env.example` for all options.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `DATABASE_URL` | — | PostgreSQL connection string |
| `WORKER_CONCURRENCY` | `10` | Delivery worker thread count |
| `MAX_RETRY_ATTEMPTS` | `9` | Attempts before DLQ (covers ~8h window) |
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
- [ ] Event ingestion API
- [ ] Endpoint registration and management
- [ ] Kafka-backed delivery worker
- [ ] Exponential backoff retry logic
- [ ] Dead letter queue
- [ ] Delivery attempt history
- [ ] HMAC-SHA256 payload signing
- [ ] Docker Compose local stack
- [ ] Prometheus metrics

### v0.2 — Operational hardening
- [ ] Per-endpoint rate limiting (protect slow consumers)
- [ ] Circuit breaker per endpoint (auto-disable consistently failing endpoints)
- [ ] Bulk event ingestion endpoint
- [ ] Endpoint health check and auto-recovery

### v0.3 — Developer experience
- [ ] Web UI for delivery monitoring and DLQ management
- [ ] CLI tool for local testing (`hookrelay send --event order.created --payload '{}'`)
- [ ] Delivery log export (CSV / JSON)
- [ ] Event schema validation support

### v0.4 — Scale and deployment
- [ ] Kubernetes Helm chart
- [ ] Horizontal worker scaling documentation
- [ ] Multi-tenancy support (isolated endpoint namespaces per API key)
- [ ] Redis-backed alternative transport layer for lightweight deployments

---

## Contributing

Contributions are welcome. For significant changes, please open an issue first to discuss scope and approach. All PRs require passing tests and updated documentation.

```bash
# Run the test suite
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=hookrelay --cov-report=term-missing
```

---

## License

MIT. See [LICENSE](LICENSE) for details.
