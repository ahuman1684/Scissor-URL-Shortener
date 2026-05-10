# Scissor — High-Throughput URL Shortener

> A production-grade URL shortener built for scale, with a Kafka-powered analytics pipeline and a React dashboard.

---

## Architecture

```
┌──────────┐   POST /api/shorten    ┌─────────────────────────────────────┐
│  Client  │ ──────────────────────▶│              Server                  │
│ React 3000│   GET /:shortCode      │  Express + ID Generator + Rate Limit │
│          │ ◀────────────────────── │                                     │
└──────────┘   302 redirect          └────────┬──────────┬────────┬────────┘
                                              │          │        │
                                           Postgres   Redis    Kafka
                                           (source)  (cache)  (events)
                                              │                  │
                                              │          ┌───────┴────────┐
                                              │          │    Consumer     │
                                              │          │  (analytics     │
                                              └──────────│   worker)       │
                                               writes    └────────────────┘
```

**Flow:**
1. Short URL creation: Server → write to Postgres + Redis (write-through)
2. Redirect: Redis hit → 302 in <1ms; miss → Postgres → populate Redis → 302
3. Every redirect: fire-and-forget Kafka event → Consumer → `click_events` + `daily_stats` + increment `urls.click_count`
4. Dashboard queries hit pre-aggregated `daily_stats` table (O(1) indexed lookup)

---

## How to Run

```bash
# 1. Clone the repo
git clone <repo-url> && cd scissor

# 2. Copy environment file
cp .env.example .env

# 3. Start everything
docker-compose up --build
```

- **API server:** http://localhost:4000
- **React client:** http://localhost:3000

---

## API Reference

### POST `/api/shorten`

Create a short URL.

**Request:**
```json
{
  "originalUrl": "https://www.example.com/very/long/path?with=params",
  "customCode": "mylink",
  "expiresIn": 7
}
```

**Response 201:**
```json
{
  "shortCode": "abc123",
  "shortUrl": "http://localhost:4000/abc123",
  "originalUrl": "https://www.example.com/...",
  "expiresAt": null,
  "createdAt": "2025-01-01T00:00:00Z"
}
```

**Errors:** `400` invalid URL · `409` code taken · `429` rate limited (10/min per IP)

---

### GET `/:shortCode`

Redirect to the original URL.

- **302** — redirect (cache hit or DB hit)
- **404** — not found
- **410** — expired

Publishes a click event to Kafka on every hit.

---

### GET `/api/analytics/:shortCode`

Per-link analytics.

```json
{
  "shortCode": "abc123",
  "originalUrl": "https://...",
  "totalClicks": 1523,
  "createdAt": "2025-01-01T00:00:00Z",
  "clicksLast7Days": [
    { "date": "2025-01-01", "clicks": 200 }
  ],
  "topReferrers": [
    { "referrer": "https://twitter.com", "count": 450 }
  ],
  "topCountries": [
    { "country": "India", "count": 800 }
  ]
}
```

---

### GET `/api/analytics/summary`

Aggregated stats across all links for the dashboard.

```json
{
  "totalLinks": 42,
  "totalClicks": 18500,
  "clicksLast7Days": [...],
  "topLinks": [
    { "shortCode": "abc123", "originalUrl": "...", "clicks": 1523 }
  ]
}
```

---

## Design Decisions

### Snowflake ID over UUID/nanoid
Snowflake IDs embed a timestamp, machine ID, and per-ms sequence counter. This gives monotonically increasing IDs (better B-tree index performance vs random UUID), zero collision risk across horizontal server instances (via machine ID), and shorter Base62 output (7-10 chars vs 21 chars for nanoid).

### Redis TTL refresh on every cache hit
On a redirect cache hit, we reset the TTL to 86400s. This is an approximation of LRU eviction at the application layer: links that are actively being clicked stay cached indefinitely; cold links expire naturally. Avoids a separate cache-warming job.

### Kafka instead of direct DB write on the redirect path
The redirect path is latency-critical (target: <10ms from cache). Writing directly to `click_events` on every redirect adds a synchronous DB round-trip. Instead, we publish a tiny JSON event fire-and-forget to Kafka (no `await`). The consumer writes to Postgres independently, keeping redirect P99 tight regardless of DB load.

### `daily_stats` pre-aggregation
The dashboard "clicks per day" query would be `SELECT date_trunc('day', clicked_at), COUNT(*) FROM click_events GROUP BY 1`. On 100K events/day × 30 days = 3M rows, this GROUP BY scan is slow. Instead, the consumer upserts into `daily_stats` on every event. Dashboard queries hit `(short_code, stat_date)` PRIMARY KEY — O(1) indexed lookup.

---

## Scaling Notes

- **Multi-node Redis:** Replace single Redis with a Redis Cluster using consistent hashing. Client uses `ioredis` Cluster mode; no code change beyond the connection string.
- **Kafka partitioning:** The `click-events` topic has 3 partitions. Add consumer instances (one per partition) for horizontal consumer scaling. Partition key on `shortCode` keeps ordering per link.
- **Postgres read replicas:** Route analytics `SELECT` queries to a read replica via `DATABASE_REPLICA_URL`. Write queries (insert click events, upsert daily_stats) stay on the primary.
- **Server horizontal scaling:** The Snowflake ID generator uses `MACHINE_ID` (0–1023). Each server instance gets a unique `MACHINE_ID` → no ID collisions, no coordination needed.
