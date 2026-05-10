# Scissor — Complete Technical Interview Preparation Guide

> Everything you need to survive (and ace) a 60-minute system design + code review interview on this project. Read this end-to-end at least once, then use the Q&A sections for rapid revision.

---

# Table of Contents

1. [Project Overview — The 30-Second Pitch](#1-project-overview)
2. [System Architecture — The Big Picture](#2-system-architecture)
3. [Why Each Technology Was Chosen](#3-technology-choices)
4. [Docker & Infrastructure Deep Dive](#4-docker--infrastructure)
5. [PostgreSQL Schema — Every Design Decision](#5-postgresql-schema)
6. [The Express Server — Full Walkthrough](#6-the-express-server)
7. [Snowflake ID Generator — How Twitter Scales IDs](#7-snowflake-id-generator)
8. [Redis Caching Strategy — Sub-10ms Redirects](#8-redis-caching-strategy)
9. [Apache Kafka — The Analytics Pipeline](#9-apache-kafka)
10. [The Kafka Consumer — Analytics Worker](#10-the-kafka-consumer)
11. [API Contract — Every Endpoint Explained](#11-api-contract)
12. [React Frontend Architecture](#12-react-frontend)
13. [Security — What We Defend Against](#13-security)
14. [Performance & Latency Analysis](#14-performance--latency)
15. [Failure Modes & Resilience](#15-failure-modes--resilience)
16. [Scaling This System to 10M+ Requests/Day](#16-scaling)
17. [Code Walkthrough — Every File Explained](#17-code-walkthrough)
18. [Interview Q&A — 80 Questions with Answers](#18-interview-qa)

---

# 1. Project Overview

## What Is Scissor?

Scissor is a **production-grade URL shortener** — the same class of system as Bitly, TinyURL, or t.co (Twitter's URL shortener). It takes a long URL like:

```
https://www.example.com/blog/2024/01/15/how-to-build-a-distributed-system-from-scratch?utm_source=newsletter&utm_medium=email&utm_campaign=jan-2024
```

...and converts it to something like:

```
http://localhost:4000/3xK9mP
```

When someone visits the short URL, they are instantly redirected to the original URL. Every redirect is also tracked: who clicked, from where, when, using what device — and all of that is aggregated into an analytics dashboard.

## What Makes This "Production-Grade"?

Most tutorial URL shorteners do `Math.random()` for the ID and store everything in a SQLite file. This system is different:

| Feature | Naive Tutorial | This System |
|---|---|---|
| ID generation | `Math.random()` / `uuid()` | Snowflake-style 64-bit (like Twitter) |
| Storage | SQLite / in-memory | PostgreSQL with proper indexes |
| Redirect speed | DB query every time | Redis cache (<1ms hits) |
| Analytics | None or synchronous | Kafka-decoupled async pipeline |
| Scale target | ~100 req/day | 100K+ redirects/day on one machine |
| Containerization | None | Full Docker Compose stack |

## The Five Core Requirements

1. **Create** a short URL from a long URL (with optional custom alias and expiry)
2. **Redirect** the short URL to the original URL in under 10ms
3. **Track** every redirect as a click event
4. **Aggregate** click events into analytics tables
5. **Display** analytics on a React dashboard

---

# 2. System Architecture

## Component Map

```
                          ┌─────────────────────────────────────┐
                          │           Docker Network             │
  Browser                 │                                      │
  ─────────               │  ┌──────────┐    ┌───────────────┐  │
  User opens              │  │  client  │    │    server     │  │
  localhost:3000  ────────┼─▶│  React   │───▶│   Express     │  │
                          │  │  :3000   │    │    :4000      │  │
                          │  └──────────┘    └──┬──┬──┬──────┘  │
                          │                     │  │  │         │
                          │            ┌────────┘  │  └──────┐  │
                          │            ▼           ▼         ▼  │
                          │       ┌─────────┐ ┌───────┐ ┌──────┐│
                          │       │Postgres │ │ Redis │ │Kafka ││
                          │       │  :5432  │ │ :6379 │ │:9092 ││
                          │       └────┬────┘ └───────┘ └──┬───┘│
                          │            │                    │    │
                          │            │      ┌─────────────┘    │
                          │            │      ▼                  │
                          │            │  ┌──────────┐           │
                          │            └─▶│ consumer │           │
                          │               │ (worker) │           │
                          │               └──────────┘           │
                          │                                      │
                          │       ┌────────────┐                 │
                          │       │ Zookeeper  │◀──── Kafka      │
                          │       │   :2181    │      metadata   │
                          │       └────────────┘                 │
                          └─────────────────────────────────────┘
```

## Data Flow — URL Shortening

```
User types URL in React form
        │
        ▼
POST /api/shorten  (HTTP to server:4000)
        │
        ▼
Validate URL + rate limit check
        │
        ▼
Generate Snowflake ID → Base62 short code
        │
        ├──────────────────────────┐
        ▼                          ▼
INSERT into urls table      SET url:<code> in Redis
(PostgreSQL)                (with 86400s TTL)
        │
        ▼
Return { shortUrl, shortCode, ... }
        │
        ▼
React shows LinkCard + stores in localStorage
```

## Data Flow — Redirect

```
User visits http://localhost:4000/3xK9mP
        │
        ▼
GET /:shortCode  hits Express server
        │
        ▼
Redis GET url:3xK9mP
        │
    ┌───┴───────────────────┐
    │ HIT                   │ MISS
    │                       ▼
    │              PostgreSQL SELECT
    │              WHERE short_code = '3xK9mP'
    │                       │
    │                  ┌────┴────┐
    │                  │ Found   │ Not found → 404
    │                  │         │ Expired   → 410
    │                  ▼
    │            Redis SET url:3xK9mP (populate cache)
    │
    ▼ (both paths converge here)
302 Redirect to originalUrl  ← happens in <1ms from cache
        │
        │ (simultaneously, fire-and-forget)
        ▼
Kafka.producer.send({ topic: 'click-events', ... })
        │  (no await — does NOT block the redirect)
        ▼
Consumer picks up message asynchronously
        │
        ├─▶ INSERT into click_events
        ├─▶ UPSERT into daily_stats
        └─▶ UPDATE urls SET click_count = click_count + 1
```

## Data Flow — Analytics Dashboard

```
React Dashboard page mounts
        │
        ▼
GET /api/analytics/summary
        │
        ▼
4 parallel PostgreSQL queries:
  ├─ COUNT(*) FROM urls
  ├─ SUM(click_count) FROM urls
  ├─ SELECT from daily_stats (last 14 days, grouped by date)
  └─ SELECT top 5 urls by click_count
        │
        ▼
JSON response → React renders StatCards + BarChart + Table
```

---

# 3. Technology Choices

## Why Node.js + Express?

**What we chose:** Node.js 18 with Express 4.

**Why Node.js specifically for a URL shortener:**

The bottleneck in a URL shortener is **I/O** — waiting for Redis, waiting for PostgreSQL, waiting for Kafka to acknowledge. Node.js uses a single-threaded event loop with non-blocking I/O. This means one Node.js process can handle thousands of concurrent redirect requests without spawning new threads, because while one request is waiting for Redis, the event loop handles other requests.

Compare to Java/Spring Boot: each request gets a dedicated thread. 10,000 concurrent requests = 10,000 threads = massive memory overhead. Node handles this with ~50 threads total.

**The event loop model:**
```
Request 1 arrives → Start Redis GET → [not blocking, go to next]
Request 2 arrives → Start Redis GET → [not blocking, go to next]
Request 3 arrives → Start Redis GET → [not blocking, go to next]
[Redis responds for Request 1] → Send 302 redirect
[Redis responds for Request 2] → Send 302 redirect
[Redis responds for Request 3] → Send 302 redirect
```

All three requests were "in flight" simultaneously even though only one thread was running.

**Why Express over alternatives (Fastify, Koa, Hapi)?**

- **Fastify** is 2x faster throughput in benchmarks. For a real production system I'd choose Fastify. But Express has the largest ecosystem and the most StackOverflow answers — good for a demo project where onboarding matters.
- **Koa** requires more boilerplate.
- **Hapi** is opinionated and best for large API teams.

Express wins for: simplicity, familiarity, ecosystem.

---

## Why PostgreSQL?

**What we chose:** PostgreSQL 16.

**The alternatives:**

| DB | Pros | Cons | Verdict |
|---|---|---|---|
| PostgreSQL | ACID, rich queries, JSON support, battle-tested | Heavier than SQLite | ✅ Our choice |
| MySQL | Fast reads, widely deployed | Less expressive SQL | Good alternative |
| SQLite | Zero setup | Not suitable for concurrent writes | Only for toys |
| MongoDB | Flexible schema | No JOIN, eventual consistency is tricky | Wrong fit |
| DynamoDB | Infinitely scalable | Expensive, complex | Overkill for this |
| CockroachDB | Distributed SQL | Complex setup | Overkill for this |

**Why PostgreSQL specifically:**

1. **ACID transactions** — when the consumer writes a click event, updates daily_stats, and increments click_count, all three writes must succeed together. PostgreSQL's `BEGIN/COMMIT/ROLLBACK` guarantees this.

2. **BIGSERIAL** — PostgreSQL's auto-incrementing 64-bit integer is perfect for our primary key.

3. **TIMESTAMPTZ** — stores timestamps with timezone info, important for analytics across global users.

4. **Upsert syntax** — `INSERT ... ON CONFLICT ... DO UPDATE` is essential for the daily_stats table. Other databases require workarounds.

5. **Index types** — PostgreSQL supports B-tree, Hash, GiST, GIN indexes. We use B-tree which is optimal for `=` and range queries on short_code and clicked_at.

6. **`pg` npm package** — well-maintained, supports connection pooling, parameter binding (prevents SQL injection), and async/await.

---

## Why Redis?

**What we chose:** Redis 7 with `ioredis` client.

**The purpose:** Redis is an **in-memory key-value store**. We use it as a read-through cache for URL lookups. Instead of hitting PostgreSQL (disk-based, ~1-5ms per query) on every redirect, we hit Redis (~0.1-0.5ms per lookup) which keeps data in RAM.

**The math:** At 100K redirects/day = ~1.2 redirects/second average. Peak might be 100x average = 120 redirects/second. PostgreSQL can handle this easily. But Redis is still valuable because:

1. **Latency** is predictably sub-millisecond from cache (P99 < 1ms vs P99 5-10ms from DB)
2. **Future-proofing** — at 10M redirects/day (115 req/sec average, 1000 req/sec peak), DB queries become expensive. Redis handles 1M ops/sec.
3. **Reduced DB load** — less DB connections, more queries available for analytics.

**Why ioredis over the `redis` npm package?**

| Feature | `redis` (node-redis) | `ioredis` |
|---|---|---|
| Cluster support | Yes (v4+) | Yes, built-in, mature |
| Sentinel support | Yes | Yes |
| Pipeline support | Yes | Yes, automatic |
| Error handling | Good | Excellent |
| Auto-reconnect | Yes | Yes, configurable strategy |
| Promise-based | Yes (v4+) | Yes |

`ioredis` was the de facto standard for years and has slightly more mature cluster/sentinel support. Both are fine choices today.

**Redis configuration we use:**
```
--maxmemory 256mb --maxmemory-policy allkeys-lru
```

- `maxmemory 256mb` — Redis will use at most 256MB of RAM.
- `allkeys-lru` — when memory is full, Redis evicts the **Least Recently Used** key regardless of whether it has a TTL. This is the right policy for a cache (we want popular URLs to stay cached, unpopular ones to be evicted).

Alternative policies:
- `noeviction` — returns errors when full. Wrong for a cache.
- `volatile-lru` — only evicts keys that have a TTL set. Since we always set TTLs, this would also work, but `allkeys-lru` is safer.
- `allkeys-random` — evicts random keys. Worse than LRU for our access pattern.

---

## Why Apache Kafka?

**What we chose:** Apache Kafka 7.5.0 (Confluent distribution) with KafkaJS client.

**The core problem Kafka solves:**

When a user visits a short URL, we need to:
1. Send the 302 redirect (must be fast, <10ms)
2. Record the click for analytics (can be slow, 50-100ms)

If we did both synchronously:
```javascript
// BAD — blocks the redirect response
await pool.query('INSERT INTO click_events ...');
await pool.query('UPDATE daily_stats ...');
await pool.query('UPDATE urls SET click_count ...');
res.redirect(302, originalUrl); // user waits 100ms+ for redirect
```

The solution is **decoupling**: the redirect path fires a Kafka event and immediately returns the redirect. A separate consumer process handles the DB writes asynchronously.

```javascript
// GOOD — redirect is instant
publishClickEvent(event); // fire-and-forget, ~1ms
return res.redirect(302, originalUrl); // user gets redirect in <10ms
```

**Why Kafka over simpler alternatives?**

| Alternative | Why Not |
|---|---|
| Direct DB write | Blocks redirect, tight coupling |
| RabbitMQ | Push-based, complex acknowledgment model |
| Redis Pub/Sub | Not persistent — if consumer is down, events are lost |
| Redis Streams | Good alternative, simpler, but less ecosystem than Kafka |
| Bull Queue (Redis-backed) | Good for job queues, but Kafka is better for event streams |
| AWS SQS | Managed service, no Kafka overhead — but not portable |
| In-process queue (Node.js array) | Events lost on server restart |

**Kafka's key properties that matter here:**

1. **Durability** — messages are written to disk. If the consumer is down for 1 hour, it catches up when it restarts. No clicks are lost.

2. **Replayability** — you can re-read old messages. If you change how analytics are calculated, you can replay all events from the beginning.

3. **Partitioning** — 3 partitions means 3 consumers can process events in parallel. This is horizontal scaling at the event processing layer.

4. **Consumer groups** — multiple consumers share the work of a partition. Add more consumer instances = more throughput.

5. **Decoupling** — the server (producer) has no knowledge of the consumer. The consumer can be updated, restarted, or replaced without touching the server.

**Kafka architecture terms you MUST know:**

- **Topic** — a named stream of events. We have `click-events`.
- **Partition** — a topic is split into N partitions for parallelism. We have 3.
- **Broker** — a Kafka server. We have 1.
- **Producer** — the server publishes to the topic.
- **Consumer** — the analytics worker reads from the topic.
- **Consumer Group** — a group of consumers that share partition assignment. Our group is `analytics-consumer-group`.
- **Offset** — the position of a consumer within a partition. Like a bookmark.
- **Zookeeper** — the coordination service Kafka depends on for leader election and configuration (being replaced by KRaft in newer Kafka versions).

**Why KafkaJS over node-rdkafka?**

`node-rdkafka` wraps the C `librdkafka` library — very fast but requires native compilation (problematic in Docker on ARM). `kafkajs` is pure JavaScript — slower but portable, easier to install, and perfectly adequate at our scale.

---

## Why Vite + React?

**What we chose:** React 18 + Vite 5.

**Vite vs Create React App (CRA):**

| | CRA (Webpack) | Vite |
|---|---|---|
| Dev server startup | 30-60 seconds | <1 second |
| HMR (hot reload) | Slow | Instant |
| Bundle tool | Webpack | Rollup (prod) + esbuild (dev) |
| Why faster | — | Uses native ES modules in dev |
| Still relevant? | Being deprecated | The current standard |

Vite uses **native ES modules** in development — the browser loads each file individually without bundling. This makes the dev server start instantly. In production, Vite uses Rollup to create optimized bundles.

**Why Recharts over alternatives?**

| Library | Approach | Pros | Cons |
|---|---|---|---|
| Recharts | React components wrapping D3 | Declarative, React-native | Opinionated API |
| Victory | Same approach | Nice API | Larger bundle |
| Chart.js | Canvas-based | Fast rendering | Requires wrapper for React |
| D3 | DOM manipulation | Infinitely flexible | Steep learning curve |
| Nivo | Recharts-like | Beautiful defaults | Very large bundle |

Recharts is the pragmatic choice: declarative (fits React's model), well-documented, and sufficient for bar/line charts.

---

# 4. Docker & Infrastructure

## Why Docker Compose?

Docker Compose lets you define a multi-container application in a single YAML file and start it with one command. Without Docker:

1. Install Postgres, configure it, create a user/database
2. Install Redis
3. Install Zookeeper
4. Install Kafka, configure it to talk to Zookeeper
5. Manage 4 different processes

With Docker Compose: `docker-compose up --build`. Everything is isolated, reproducible, and tears down cleanly with `docker-compose down`.

## The Docker Compose File — Line by Line

```yaml
version: '3.9'
```
Specifies the Compose file format version. 3.9 supports the `condition` syntax in `depends_on` (required for health check dependencies).

### Postgres Service

```yaml
postgres:
  image: postgres:16-alpine
  environment:
    POSTGRES_USER: scissor
    POSTGRES_PASSWORD: scissor123
    POSTGRES_DB: scissor
  ports: ['5432:5432']
  volumes: [pgdata:/var/lib/postgresql/data]
  healthcheck:
    test: ['CMD-SHELL', 'pg_isready -U scissor']
    interval: 5s
    timeout: 5s
    retries: 10
```

- `postgres:16-alpine` — Alpine Linux base image. Much smaller than the default Debian image (~85MB vs ~400MB).
- `POSTGRES_USER/PASSWORD/DB` — the init script creates this user and database on first startup.
- `ports: ['5432:5432']` — maps host port 5432 to container port 5432, allowing access from your local machine (useful for DB GUIs like TablePlus).
- `volumes: [pgdata:/var/lib/postgresql/data]` — persists data to a named Docker volume. Without this, all data is lost when the container stops.
- `pg_isready -U scissor` — built-in Postgres utility that checks if the server is accepting connections. Returns exit code 0 when ready, 1 when not.

### Redis Service

```yaml
redis:
  image: redis:7-alpine
  command: redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru
  ports: ['6379:6379']
  healthcheck:
    test: ['CMD', 'redis-cli', 'ping']
```

- `command` overrides the default Redis startup command to add our memory limits.
- `redis-cli ping` — sends the PING command to Redis; returns PONG on success.

### Zookeeper Service

```yaml
zookeeper:
  image: confluentinc/cp-zookeeper:7.5.0
  environment:
    ZOOKEEPER_CLIENT_PORT: 2181
    ZOOKEEPER_TICK_TIME: 2000
  healthcheck:
    test: ['CMD', 'bash', '-c', 'echo > /dev/tcp/localhost/2181']
    interval: 5s
    timeout: 5s
    retries: 15
    start_period: 20s
```

**Why Zookeeper at all?**

Kafka (pre-KRaft mode, which is what version 7.5.0 uses) requires Zookeeper for:
- Broker leader election (which broker is the "leader" for each partition)
- Storing cluster metadata (topic configs, partition assignments)
- Consumer group coordination

Newer Kafka versions (3.3+) support **KRaft mode** — Kafka manages its own metadata without Zookeeper. But the Confluent 7.5.0 image uses Zookeeper by default.

**The health check: `echo > /dev/tcp/localhost/2181`**

This uses bash's built-in TCP connectivity feature. `/dev/tcp/host/port` is a virtual file — when bash opens it, it creates a TCP connection. `echo >` redirects echo's output to that connection. If the connection succeeds (Zookeeper is listening on 2181), exit code is 0 (healthy). If the connection is refused, bash cannot open the file, exit code is non-zero (unhealthy).

This is more reliable than `nc` (netcat) because `nc` isn't always installed, and its `-w` timeout flag syntax varies between distributions.

**`start_period: 20s`** — the container won't be marked unhealthy during the first 20 seconds, even if checks fail. This is critical because the Confluent Zookeeper image takes ~10-15 seconds for the JVM to start and bind port 2181.

### Kafka Service

```yaml
kafka:
  image: confluentinc/cp-kafka:7.5.0
  depends_on:
    zookeeper:
      condition: service_healthy
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
  healthcheck:
    test: ['CMD', 'kafka-topics', '--bootstrap-server', 'localhost:9092', '--list']
    interval: 10s
    timeout: 15s
    retries: 20
    start_period: 45s
```

**Understanding the listener configuration — this is the trickiest part:**

Kafka has two types of listeners:

1. **Internal listener** (`PLAINTEXT://kafka:29092`) — used by other Docker containers (server, consumer) to reach Kafka. They use the Docker hostname `kafka` which resolves within the Docker network.

2. **External listener** (`PLAINTEXT_HOST://localhost:9092`) — used by your local machine (e.g., a Kafka GUI tool, or testing with `kafka-console-producer`). It uses `localhost:9092` which is mapped by Docker's port binding.

Why two ports? When Kafka sends you metadata (like "here's where to connect for partition 0"), it sends the `ADVERTISED_LISTENERS`. If it only advertised `kafka:29092`, your laptop couldn't connect because `kafka` doesn't resolve outside Docker. If it only advertised `localhost:9092`, containers couldn't connect reliably.

`KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT` tells Kafka brokers to communicate with each other via the PLAINTEXT listener (port 29092).

`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` — Kafka has an internal topic (`__consumer_offsets`) that tracks where each consumer group is in each partition. With only 1 broker, this topic can only have replication factor 1 (can't replicate with 1 broker). This tells Kafka not to require multiple replicas.

**The `kafka-topics --list` health check:**

This command actually connects to the Kafka broker and lists all topics. It succeeds only when Kafka is fully up and accepting connections — not just when the port is open. This is why it needs `start_period: 45s` — Kafka takes longer to start than Zookeeper because it has to:
1. Start the JVM
2. Connect to Zookeeper
3. Wait for Zookeeper to elect it as broker leader
4. Initialize internal topics
5. Begin accepting client connections

### Server & Consumer Services

```yaml
server:
  build: ./server
  depends_on:
    postgres:
      condition: service_healthy
    redis:
      condition: service_healthy
    kafka:
      condition: service_healthy
  restart: on-failure
```

`build: ./server` — Docker builds the image from `./server/Dockerfile` instead of pulling from a registry.

`depends_on` with `condition: service_healthy` — the server container won't start until Postgres, Redis, AND Kafka all pass their health checks. Without this, the server might start and immediately crash because Kafka isn't ready.

`restart: on-failure` — if the server crashes (non-zero exit code), Docker restarts it automatically. This is our fallback in case the health checks aren't quite long enough.

## How Docker Networking Works

All services in the same `docker-compose.yml` share a default network. Docker creates a DNS resolver within that network. The hostname `postgres` resolves to the Postgres container's IP. `kafka` resolves to Kafka's IP. This is why `DATABASE_URL: postgresql://...@postgres:5432/scissor` works — `postgres` is the service name in docker-compose.yml, and Docker's internal DNS resolves it.

## Dockerfiles — Multi-Stage vs Single Stage

Our Dockerfiles are simple single-stage:

```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --production
COPY . .
EXPOSE 4000
CMD ["node", "src/index.js"]
```

**Why copy `package*.json` first, then `npm install`, then copy the rest?**

This is **layer caching**. Docker builds images in layers. Each instruction creates a layer. If a layer's input hasn't changed, Docker reuses the cached layer.

If you copy everything first then run npm install:
```dockerfile
COPY . .            # If ANY file changes, this layer is invalidated
RUN npm install     # Always re-runs even if package.json didn't change
```

If you copy package.json first:
```dockerfile
COPY package*.json ./  # Only invalidated if package.json changes
RUN npm install        # Only re-runs when dependencies change
COPY . .               # Invalidated when source code changes
```

This means changing a `.js` file doesn't trigger `npm install` on the next build — it reuses the cached `node_modules` layer. Build time drops from ~60s to ~5s.

**`npm install --production`** — skips devDependencies (like nodemon, TypeScript, etc.). Keeps the Docker image smaller.

**Node 18 Alpine** — Alpine Linux is a minimal Linux distribution (~5MB). The `node:18-alpine` image is ~150MB vs `node:18` (Debian) at ~1GB.

---

# 5. PostgreSQL Schema

## The Three Tables

### Table 1: `urls` — The Source of Truth

```sql
CREATE TABLE IF NOT EXISTS urls (
  id            BIGSERIAL PRIMARY KEY,
  short_code    VARCHAR(12) UNIQUE NOT NULL,
  original_url  TEXT NOT NULL,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  expires_at    TIMESTAMPTZ,
  click_count   BIGINT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_urls_short_code ON urls(short_code);
```

**Design decisions:**

`id BIGSERIAL` — auto-incrementing 64-bit integer. We have this as the PK even though `short_code` is unique because:
1. It's a smaller, faster join key for any future foreign keys
2. BIGSERIAL is sequential (better B-tree performance than random strings)

`short_code VARCHAR(12)` — our Snowflake IDs in Base62 produce 7-10 character strings. VARCHAR(12) gives a small safety margin. UNIQUE constraint + NOT NULL enforces our invariant.

`original_url TEXT` — TEXT has no length limit in PostgreSQL. URLs can theoretically be thousands of characters.

`created_at TIMESTAMPTZ DEFAULT NOW()` — TIMESTAMPTZ stores UTC timestamp with timezone offset. Always use TIMESTAMPTZ over TIMESTAMP for user-facing data. `DEFAULT NOW()` is set by the database, not the application, which means it's correct even if the server clock is wrong.

`expires_at TIMESTAMPTZ` — nullable. NULL means "never expires". This is a clean design — no magic values like `-1` or `9999-12-31`.

`click_count BIGINT DEFAULT 0` — **denormalized counter**. This is the same count you could get with `SELECT COUNT(*) FROM click_events WHERE short_code = x`, but pre-computed. A direct query on click_events would be slow as it grows. This counter is incremented by the consumer on every click.

**Why the index on short_code if it's already UNIQUE?**

PostgreSQL automatically creates a unique index on UNIQUE constraint columns. The `CREATE INDEX` statement here is technically redundant — PostgreSQL already made the index. Writing it explicitly makes the schema self-documenting. In production, you'd remove the explicit `CREATE INDEX` since it's a no-op.

### Table 2: `click_events` — The Raw Event Log

```sql
CREATE TABLE IF NOT EXISTS click_events (
  id            BIGSERIAL PRIMARY KEY,
  short_code    VARCHAR(12) NOT NULL,
  clicked_at    TIMESTAMPTZ NOT NULL,
  ip_address    VARCHAR(45),
  user_agent    TEXT,
  referrer      TEXT,
  country       VARCHAR(60)
);

CREATE INDEX IF NOT EXISTS idx_clicks_short_code ON click_events(short_code);
CREATE INDEX IF NOT EXISTS idx_clicks_clicked_at ON click_events(clicked_at);
```

**Design decisions:**

This table is an **append-only log** — events are only inserted, never updated. This is the raw data. Think of it like server access logs.

`ip_address VARCHAR(45)` — why 45? IPv4 addresses are up to 15 chars (`255.255.255.255`). IPv6 addresses are up to 39 chars (`2001:0db8:85a3:0000:0000:8a2e:0370:7334`). But IPv4-mapped IPv6 addresses (::ffff:255.255.255.255) are 45 chars. So 45 is the maximum you'll ever need.

`user_agent TEXT` — user agent strings can be very long ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36..."). TEXT has no practical limit.

`referrer TEXT` — the HTTP Referer header. Also potentially long.

`country VARCHAR(60)` — country name. "United States of America" is 24 chars. "Democratic Republic of the Congo" is 32 chars. 60 is safe.

**Two indexes:**

1. `idx_clicks_short_code` — for queries like "give me all clicks for short code abc123"
2. `idx_clicks_clicked_at` — for time-range queries like "give me all clicks in the last 7 days"

These two indexes support the analytics queries efficiently.

**Why not a foreign key from click_events.short_code to urls.short_code?**

Foreign keys enforce referential integrity but add overhead on every INSERT. Since the consumer processes high-throughput click events, the extra constraint check on every insert would slow it down. Also, if a URL is deleted, we might still want to keep click history.

### Table 3: `daily_stats` — The Pre-Aggregated Cache

```sql
CREATE TABLE IF NOT EXISTS daily_stats (
  short_code    VARCHAR(12) NOT NULL,
  stat_date     DATE NOT NULL,
  click_count   INT DEFAULT 0,
  PRIMARY KEY (short_code, stat_date)
);
```

**This is the most important design decision in the entire schema.**

**The problem with querying click_events directly:**

```sql
-- To get clicks per day for the dashboard, naive approach:
SELECT DATE(clicked_at), COUNT(*)
FROM click_events
WHERE clicked_at > NOW() - INTERVAL '14 days'
GROUP BY DATE(clicked_at)
ORDER BY DATE(clicked_at);
```

At 100K clicks/day × 14 days = 1.4M rows scanned. At 1M clicks/day × 14 days = 14M rows scanned. This is an O(n) scan even with the `clicked_at` index, because GROUP BY requires aggregating all matching rows.

**The solution — pre-aggregation:**

Instead of computing the answer at query time, we compute it at write time (in the consumer). Every click event triggers an UPSERT:

```sql
INSERT INTO daily_stats (short_code, stat_date, click_count)
VALUES ('abc123', '2024-01-15', 1)
ON CONFLICT (short_code, stat_date)
DO UPDATE SET click_count = daily_stats.click_count + 1;
```

Now the dashboard query is:
```sql
SELECT stat_date, SUM(click_count)
FROM daily_stats
WHERE stat_date >= NOW() - INTERVAL '14 days'
GROUP BY stat_date
ORDER BY stat_date;
```

This scans at most 14 rows per unique date (since daily_stats has one row per code per day). The PRIMARY KEY `(short_code, stat_date)` is a composite key that serves as both the uniqueness constraint AND the index. Dashboard queries become O(1) indexed lookups.

**The trade-off:** We do more work at write time (insert + upsert instead of just insert) but save enormous work at read time. This is the classic **write amplification for read optimization** pattern, used by virtually every analytics system at scale (Druid, ClickHouse, BigQuery all use this approach).

**`ON CONFLICT ... DO UPDATE`** — PostgreSQL's upsert syntax. If a row with `(short_code, stat_date)` already exists, increment the count. If not, insert a new row with count = 1. This is atomic — no race condition between check-and-insert.

---

# 6. The Express Server

## Application Bootstrap (`server/src/index.js`)

```javascript
const express = require('express');
const cors = require('cors');
const pool = require('./config/db');
const migrate = require('./db/migrate');
const { init: initKafkaProducer } = require('./services/kafkaProducer');
const errorHandler = require('./middleware/errorHandler');

const app = express();

app.set('trust proxy', 1);
app.use(cors());
app.use(express.json());

app.use('/api/shorten', require('./routes/shorten'));
app.use('/api/analytics', require('./routes/analytics'));
app.use('/', require('./routes/redirect'));
app.use(errorHandler);
```

**`app.set('trust proxy', 1)`** — This tells Express to trust the `X-Forwarded-For` HTTP header set by a reverse proxy (like nginx or a load balancer). This makes `req.ip` return the client's real IP address instead of the proxy's IP. Without this, the rate limiter would rate-limit the proxy, not the actual user. The `1` means "trust the first proxy in the chain."

**Middleware order matters:**

1. `trust proxy` — must be first, before any middleware reads `req.ip`
2. `cors()` — must be before routes so OPTIONS preflight requests get CORS headers
3. `express.json()` — must be before routes so `req.body` is populated

**Route order matters:**

```javascript
app.use('/api/shorten', require('./routes/shorten'));   // registered first
app.use('/api/analytics', require('./routes/analytics')); // registered second
app.use('/', require('./routes/redirect'));              // registered last
```

This is critical. The redirect route `GET /:shortCode` would match ANY path with one segment — including `/api`. By registering API routes first, Express handles `/api/shorten` with the shorten router before the redirect router gets a chance to intercept it.

If you reversed the order:
```javascript
app.use('/', require('./routes/redirect'));       // catches everything
app.use('/api/shorten', ...);                    // never reached
```

**`errorHandler` as the last `app.use()`** — Express identifies an error handler by its 4-parameter signature `(err, req, res, next)`. It must be registered after all routes so unhandled errors from any route propagate to it.

## Startup Sequence with Retry

```javascript
async function waitForPostgres(retries = 20, delayMs = 3000) {
  for (let i = 0; i < retries; i++) {
    try {
      const client = await pool.connect();
      client.release();
      console.log('PostgreSQL connected');
      return;
    } catch (err) {
      console.log(`PostgreSQL not ready (${i + 1}/${retries})… retrying in ${delayMs}ms`);
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
  throw new Error('Could not connect to PostgreSQL after multiple retries');
}

async function start() {
  await waitForPostgres();
  await migrate();
  await initKafkaProducer();
  app.listen(port, ...);
}
```

Even with Docker healthchecks, there's a race: the healthcheck says "ready" the moment Postgres starts accepting connections, but the `pg` client pool might have a slightly different timing. The retry loop is an application-level safety net.

Why not use connection pool's built-in retry? The pool's retry is per-query. We want to fail fast at startup rather than silently retrying on every request. Explicit retry at startup gives clear log output.

---

# 7. Snowflake ID Generator

## Why Not UUID?

```javascript
// uuid approach:
const { v4: uuidv4 } = require('uuid');
const id = uuidv4(); // "550e8400-e29b-41d4-a716-446655440000"
```

Problems with UUID:
1. **Length** — 36 characters with hyphens, 32 without. Too long for a URL.
2. **Randomness** — v4 UUIDs are random, making the B-tree index fragmented. Every new UUID inserts at a random position in the index tree, causing page splits and cache misses. Sequential IDs insert at the end, keeping the index tight.
3. **Not Base62-friendly** — converting a UUID to a short Base62 string is awkward.

## Why Not nanoid?

```javascript
const { nanoid } = require('nanoid');
const id = nanoid(8); // "V1StGXR8"
```

Problems with nanoid:
1. **Not collision-resistant at scale** — with 8 characters and 64-character alphabet, you need ~57,000 IDs before collision probability exceeds 1%. Fine for most use cases, but Snowflake IDs are mathematically guaranteed unique.
2. **Not sortable** — nanoid is random, not time-ordered.
3. **Coordination required** — for true uniqueness across machines, you need a coordination service or collision detection logic.

## How the Snowflake Algorithm Works

```
 ┌────────────────────────────────────────────────────────────────┐
 │                     64-bit Snowflake ID                        │
 ├─────────────────────────────┬──────────────┬───────────────────┤
 │  41 bits: timestamp (ms)    │ 10 bits:     │ 12 bits:          │
 │  since custom epoch         │ machine ID   │ sequence counter  │
 └─────────────────────────────┴──────────────┴───────────────────┘
```

**41 bits of timestamp:**
- 2^41 milliseconds = ~69 years from the epoch
- Our epoch is November 2023, so IDs are valid until ~2092
- Gives us millisecond precision

**10 bits of machine ID:**
- 2^10 = 1024 possible machine IDs
- Allows up to 1024 server instances with zero coordination
- Set via `MACHINE_ID` environment variable

**12 bits of sequence:**
- 2^12 = 4096 unique IDs per millisecond per machine
- If we hit 4096 IDs in a millisecond, we wait for the next millisecond
- Gives us 4096 × 1000 = 4.096 million IDs per second per machine

**Total guaranteed throughput:** 1024 machines × 4.096M IDs/sec = **4.2 billion unique IDs per second** with zero coordination.

## The Code Explained

```javascript
const ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
```
62-character Base62 alphabet. 62 = 10 digits + 26 lowercase + 26 uppercase.

```javascript
const MACHINE_ID = parseInt(process.env.MACHINE_ID || '1'); // 0-1023
```
Read from environment. In Docker Compose, we set `MACHINE_ID: 1`. If you run two server instances, give them `MACHINE_ID: 1` and `MACHINE_ID: 2`.

```javascript
const EPOCH = 1700000000000n; // custom epoch (Nov 2023)
```
BigInt literal (the `n` suffix). We subtract this from the current timestamp so our IDs are as short as possible. Using the Unix epoch (1970) would waste bits on the 53+ years before our service existed.

```javascript
let sequence = 0n;
let lastTimestamp = -1n;
```
Module-level state. In Node.js, each module is loaded once (singleton). These variables persist across function calls, which is exactly what we need for the sequence counter.

```javascript
function generateId() {
  let timestamp = BigInt(Date.now()) - EPOCH;
```
`Date.now()` returns milliseconds since Unix epoch as a regular JavaScript number. We subtract our custom epoch and convert to BigInt for bitwise operations.

```javascript
  if (timestamp === lastTimestamp) {
    sequence = (sequence + 1n) & 0xFFFn;
```
If we're still in the same millisecond as the last call, increment the sequence. `& 0xFFFn` masks to 12 bits (0xFFF = 4095 = 2^12 - 1). If sequence was 4095 and we add 1, `4096 & 4095 = 0` — it wraps around.

```javascript
    if (sequence === 0n) {
      // wait for next ms
      while (BigInt(Date.now()) - EPOCH <= lastTimestamp) {}
      timestamp = BigInt(Date.now()) - EPOCH;
    }
```
If we've exhausted all 4096 sequence numbers in this millisecond, spin-wait until the next millisecond. This is a busy-wait (CPU spins), which is acceptable here because it lasts at most 1 millisecond and happens only under extreme load (4096+ IDs/ms = 4M+ IDs/second).

```javascript
  } else {
    sequence = 0n;
  }
  lastTimestamp = timestamp;
  const id = (timestamp << 22n) | (BigInt(MACHINE_ID) << 12n) | sequence;
```

Building the 64-bit integer:
- `timestamp << 22n` — shift timestamp left by 22 bits (making room for 10-bit machine ID + 12-bit sequence)
- `BigInt(MACHINE_ID) << 12n` — shift machine ID left by 12 bits (making room for sequence)
- `| sequence` — OR in the sequence number
- `|` is bitwise OR, combining all three parts into one 64-bit integer

```javascript
  return toBase62(id);
}

function toBase62(num) {
  let result = '';
  while (num > 0n) {
    result = ALPHABET[Number(num % 62n)] + result;
    num = num / 62n;
  }
  return result || '0';
}
```

Base conversion algorithm (same as converting decimal to binary, but base 62):
1. Take the remainder when dividing by 62 → maps to a character in the alphabet
2. Prepend to result (prepend, not append, so most-significant digit comes first)
3. Divide by 62 and repeat
4. Stop when num is 0

Example: converting 100 to base 62:
- 100 % 62 = 38 → 'C' (index 38 in alphabet: 10 digits + 26 lower + C is 2nd uppercase = 38)
- 100 / 62 = 1 (integer division)
- 1 % 62 = 1 → '1'
- 1 / 62 = 0 → stop
- Result: "1C"

---

# 8. Redis Caching Strategy

## The Cache Service

```javascript
const PREFIX = 'url:';
const TTL = 86400; // 24 hours in seconds

async function getUrl(shortCode) {
  return redis.get(PREFIX + shortCode);
}

async function setUrl(shortCode, url) {
  await redis.set(PREFIX + shortCode, url, 'EX', TTL);
}

async function invalidate(shortCode) {
  await redis.del(PREFIX + shortCode);
}
```

**Why prefix the keys with `url:`?**

Redis has a flat key namespace. Without prefixes, `abc123` might conflict with other keys in the future. With `url:abc123`, all URL cache entries are clearly namespaced. This also allows pattern-based operations like `KEYS url:*` for debugging.

**The `EX 86400` option:**

Redis TTL (Time To Live). After 86400 seconds (24 hours), Redis automatically deletes the key. This serves two purposes:
1. **Stale data prevention** — if a URL is updated or deleted in PostgreSQL, the cache entry expires within 24 hours
2. **Memory management** — cold links (not clicked for 24h) are evicted from Redis, freeing memory for hot links

## Write-Through Caching

```javascript
// In shortenController.js:
const result = await pool.query('INSERT INTO urls ...');
await setUrl(shortCode, originalUrl); // write to cache immediately
```

When creating a new short URL, we write to BOTH PostgreSQL AND Redis simultaneously. This is called **write-through caching**.

Alternative: **write-around** — only write to PostgreSQL, populate Redis on first redirect. The first redirect would be slower (cache miss → DB query), but subsequent redirects would be fast.

Write-through is better here because the first redirect is likely from the person who just created the link, and they expect instant response.

## TTL Refresh on Cache Hit

```javascript
// In redirectController.js:
let originalUrl = await getUrl(shortCode);

if (originalUrl) {
  // Cache hit — refresh TTL (LRU approximation at application layer)
  setUrl(shortCode, originalUrl).catch(() => {});
```

When we serve a cached redirect, we reset the TTL to 86400s. This means **frequently accessed links stay cached indefinitely**. A link that gets 100 clicks/day will always have its TTL reset before it expires.

Only links that go unclicked for 24 hours will expire from cache. On their next click, they'll be a cache miss (DB query), but then cached again.

This is an **LRU (Least Recently Used) approximation**:
- LRU = evict the least recently used item
- Our TTL refresh = items that are used recently have longer effective cache life

Redis's `allkeys-lru` eviction policy adds a second layer of LRU: if Redis fills up, it evicts the key that was least recently accessed (Redis tracks this with its own LRU clock). This works hand-in-hand with our TTL refresh.

Notice `setUrl(...).catch(() => {})` — we don't `await` the TTL refresh because:
1. It's not critical to the redirect path
2. If it fails, the TTL just won't be refreshed (link expires in <24h — acceptable)
3. Not awaiting it means the redirect responds faster

## Cache Miss Flow

```javascript
const result = await pool.query(
  'SELECT original_url, expires_at FROM urls WHERE short_code = $1',
  [shortCode]
);

if (result.rows.length === 0) {
  return res.status(404).json({ error: 'Short URL not found' });
}

const row = result.rows[0];
if (row.expires_at && new Date(row.expires_at) < new Date()) {
  return res.status(410).json({ error: 'This link has expired' });
}

originalUrl = row.original_url;
setUrl(shortCode, originalUrl).catch(() => {}); // populate cache
```

On a cache miss, we check PostgreSQL. We also check expiry — if the link has expired, we return 410 Gone (not 404, because the resource existed, it just expired). 410 is more semantically correct than 404 for expired content.

**Why don't we cache expired URLs?**

An expired URL should never be served. If we cached it, even with a short TTL, someone could visit an "expired" link and be redirected. So we never call `setUrl` for expired URLs.

## Redis Connection Configuration

```javascript
const redis = new Redis(process.env.REDIS_URL, {
  retryStrategy: (times) => Math.min(times * 200, 5000),
  maxRetriesPerRequest: null,
  lazyConnect: false,
});
```

`retryStrategy` — if the connection drops, ioredis will retry. The backoff starts at 200ms, doubles... wait, it's `times * 200`, so: 200ms, 400ms, 600ms... up to max 5000ms. Linear backoff (not exponential).

`maxRetriesPerRequest: null` — commands queued while disconnected will keep retrying indefinitely (until reconnected). Alternative: `maxRetriesPerRequest: 3` would fail commands after 3 retries.

`lazyConnect: false` — connect immediately when creating the client, not on first command. This ensures connection errors surface at startup.

---

# 9. Apache Kafka

## Topic Configuration

```javascript
// In kafkaProducer.js:
await admin.createTopics({
  topics: [{ topic: 'click-events', numPartitions: 3, replicationFactor: 1 }],
  waitForLeaders: true,
});
```

**`numPartitions: 3`** — why 3?

Partitions are the unit of parallelism in Kafka. With 3 partitions, you can run up to 3 consumer instances processing events simultaneously. The rule of thumb: number of partitions = max number of consumers you expect to run.

We chose 3 because it's a reasonable starting point — not too few (limits future scaling) and not too many (each partition has overhead).

**`replicationFactor: 1`** — normally in production you'd set this to 3 (each partition is replicated to 3 brokers for fault tolerance). With a single broker (our dev setup), replication factor can only be 1.

**`waitForLeaders: true`** — after creating the topic, wait until Kafka elects partition leaders before returning. Without this, producing messages immediately after topic creation might fail because no leader has been elected yet.

## Producer Configuration

```javascript
const kafka = new Kafka({
  clientId: 'scissor-server',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  retry: {
    retries: 20,
    initialRetryTime: 1000,
    maxRetryTime: 15000,
  },
});
```

`clientId` — used for logging and monitoring. Shows up in Kafka logs as the client identifier.

`retry` — if producing a message fails (broker unavailable, network issue), KafkaJS retries up to 20 times with exponential backoff starting at 1s, capped at 15s.

## Fire-and-Forget Publishing

```javascript
function publishClickEvent(event) {
  if (!producer) return;
  producer.send({
    topic: 'click-events',
    messages: [{ value: JSON.stringify(event) }],
  }).catch((err) => console.error('Kafka publish error:', err.message));
}
```

**Critical design choice: no `await`**

We call `publishClickEvent` without `await`. This means:
- The redirect response is sent immediately after the Redis hit
- The Kafka publish happens asynchronously in the background
- If Kafka is temporarily unavailable, the redirect still works (the publish will be retried by KafkaJS's retry logic)

The downside: if the server crashes after sending the redirect but before the Kafka message is acknowledged, that click event is lost. This is an acceptable trade-off — losing a few click events occasionally is far better than making every redirect wait for Kafka acknowledgment.

The `.catch()` ensures unhandled promise rejections don't crash the process.

## Consumer Group Semantics

```javascript
const consumer = kafka.consumer({ groupId: 'analytics-consumer-group' });
await consumer.subscribe({ topic: 'click-events', fromBeginning: false });
```

`groupId` — identifies the consumer group. All consumers with the same groupId share the partitions of a topic. With 3 partitions and 1 consumer instance, the consumer gets all 3 partitions. With 3 consumer instances, each gets 1 partition.

`fromBeginning: false` — start consuming from new messages only. If you set `fromBeginning: true`, the consumer would replay ALL historical messages from the beginning of the topic (useful for reprocessing). For a fresh deployment, we start from "now".

## Partition Assignment Example

With 3 partitions and 1 consumer:
```
Partition 0 → Consumer Instance A
Partition 1 → Consumer Instance A
Partition 2 → Consumer Instance A
```

With 3 partitions and 2 consumers:
```
Partition 0 → Consumer Instance A
Partition 1 → Consumer Instance A
Partition 2 → Consumer Instance B
```

With 3 partitions and 3 consumers:
```
Partition 0 → Consumer Instance A
Partition 1 → Consumer Instance B
Partition 2 → Consumer Instance C
```

With 4 partitions and 3 consumers (one consumer gets 2):
```
Partition 0 → Consumer Instance A
Partition 1 → Consumer Instance A
Partition 2 → Consumer Instance B
Partition 3 → Consumer Instance C
```

## Message Format

```json
{
  "shortCode": "3xK9mP",
  "clickedAt": "2024-01-15T14:30:00.000Z",
  "ipAddress": "1.2.3.4",
  "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...",
  "referrer": "https://twitter.com"
}
```

The message `value` is a JSON string. The consumer calls `JSON.parse(message.value.toString())` to deserialize. We could use Avro or Protobuf for a schema-enforced, binary-encoded format (smaller, faster, schema evolution support), but JSON is simpler for this project.

## Offset Management

Kafka tracks where each consumer group is in each partition using **offsets**. An offset is just a sequential integer — message 0, message 1, message 2, etc.

When a consumer processes a message, it **commits the offset** — telling Kafka "I've processed up to message N." If the consumer restarts, it resumes from the last committed offset.

KafkaJS auto-commits offsets by default (after each batch). Our code uses `eachMessage` which commits the offset automatically after the callback returns.

```javascript
await consumer.run({
  eachMessage: async ({ message }) => {
    try {
      await handleClick(message);
    } catch (err) {
      console.error('Error processing click event:', err.message);
      // We don't re-throw — offset is committed even on error
    }
  },
});
```

**The error handling strategy:**

If `handleClick` throws, we log the error but don't re-throw. This means the offset IS committed (KafkaJS commits after the callback, whether it threw or not — actually, KafkaJS won't commit if the callback throws... let me clarify):

In KafkaJS's `eachMessage` mode with auto-commit, the offset is committed when the `eachMessage` promise resolves (not rejects). By catching the error inside the callback and not re-throwing, the promise resolves successfully, and the offset is committed.

**Why commit the offset even on error?**

The alternative (letting it throw → offset not committed → retry) could cause an **infinite retry loop** if the message is malformed. Malformed messages would block all subsequent processing indefinitely.

Our choice: **idempotency over at-least-once**. Accept that some clicks might not be recorded (if the DB write fails). This is better than blocking the pipeline.

A production system would send failed messages to a **Dead Letter Queue (DLQ)** — another Kafka topic where failed messages accumulate for manual inspection and reprocessing.

---

# 10. The Kafka Consumer

## Full Click Processing Pipeline

```javascript
// consumer/src/handlers/clickHandler.js

const ipCache = new Map();

async function getCountry(ip) {
  if (!ip || ip === '127.0.0.1' || ip === '::1' || ip.startsWith('172.') || ip.startsWith('10.')) {
    return null;
  }
  if (ipCache.has(ip)) return ipCache.get(ip);

  try {
    const { data } = await axios.get(`http://ip-api.com/json/${ip}`, { timeout: 3000 });
    const country = data.status === 'success' ? data.country : null;
    ipCache.set(ip, country);
    return country;
  } catch {
    return null;
  }
}
```

**IP Geolocation with ip-api.com:**

`ip-api.com` is a free API that returns geolocation data for an IP address. No API key required. Rate limit: 45 requests/minute on the free tier. At 100K clicks/day with a good cache hit rate, we use far fewer than 45 API calls/minute.

**The local `ipCache` Map:**

This is an in-process cache. If the same IP makes 100 clicks, we only call ip-api.com once for that IP — subsequent lookups are served from the Map.

Limitation: the Map grows unboundedly. For a long-running consumer, this could be a memory leak. A production fix: use a LRU cache (like the `lru-cache` npm package) with a max size:

```javascript
const LRU = require('lru-cache');
const ipCache = new LRU({ max: 10000 }); // keep last 10K IPs
```

**Why skip private IPs?**

`127.0.0.1` (localhost), `::1` (IPv6 localhost), `172.x.x.x` (Docker network), `10.x.x.x` (private network) — these are not real user IPs. ip-api.com would return an error for them. We short-circuit with `return null`.

## The Transaction

```javascript
const client = await pool.connect();
try {
  await client.query('BEGIN');

  await client.query(
    `INSERT INTO click_events (short_code, clicked_at, ip_address, user_agent, referrer, country)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [shortCode, clickedAt, ipAddress, userAgent, referrer, country]
  );

  await client.query(
    `INSERT INTO daily_stats (short_code, stat_date, click_count)
     VALUES ($1, $2, 1)
     ON CONFLICT (short_code, stat_date)
     DO UPDATE SET click_count = daily_stats.click_count + 1`,
    [shortCode, statDate]
  );

  await client.query(
    'UPDATE urls SET click_count = click_count + 1 WHERE short_code = $1',
    [shortCode]
  );

  await client.query('COMMIT');
} catch (err) {
  await client.query('ROLLBACK');
  throw err;
} finally {
  client.release();
}
```

**Why a transaction?**

All three writes must either ALL succeed or ALL fail together. If we insert into `click_events` but then the `daily_stats` upsert fails, we'd have a click counted in the raw log but NOT in the aggregation table — inconsistency. The transaction rolls back all writes if any fails.

**`pool.connect()` vs `pool.query()`:**

`pool.query()` checks out a client, runs the query, and returns it. Fine for single queries.

`pool.connect()` checks out a client and keeps it. Required for transactions — all queries in a transaction must use the same client connection (transactions are connection-scoped in PostgreSQL).

`client.release()` in `finally` — always release the client back to the pool, even if an error occurred. Forgetting this causes connection pool exhaustion.

**Parameter binding:**

```javascript
pool.query('SELECT ... WHERE short_code = $1', [shortCode])
```

`$1`, `$2`, etc. are **parameterized queries**. The `pg` library sends the SQL and parameters separately to PostgreSQL, which compiles the query plan once and substitutes the parameters safely. This prevents SQL injection — user-provided data (like the short code from a URL) is treated as data, never as SQL code.

**Never do this:**
```javascript
pool.query(`SELECT ... WHERE short_code = '${shortCode}'`)
// If shortCode = "'; DROP TABLE urls; --"  → SQL injection!
```

---

# 11. API Contract

## POST `/api/shorten`

### Rate Limiter

```javascript
// rateLimiter.js
function rateLimiter(limit, windowMs) {
  return (req, res, next) => {
    const ip = req.ip || req.socket.remoteAddress;
    const now = Date.now();
    const windowStart = now - windowMs;

    const times = (requests.get(ip) || []).filter((t) => t > windowStart);
    if (times.length >= limit) {
      return res.status(429).json({ error: 'Rate limit exceeded.' });
    }
    times.push(now);
    requests.set(ip, times);
    next();
  };
}
```

This is a **sliding window rate limiter**. It keeps a list of timestamps for each IP. On each request:
1. Filter out timestamps older than the window (now - 60s)
2. If the count >= limit, reject with 429
3. Otherwise, add current timestamp and allow

**Sliding window vs fixed window:**

Fixed window: "You get 10 requests per minute, resetting at :00, :01, :02..."

Problem: a user can make 10 requests at :59, and 10 more at :01 = 20 requests in 2 seconds.

Sliding window: "You get 10 requests in any 60-second period"

Sliding window is fairer and prevents burst exploitation.

**Limitation:** This is an in-memory rate limiter. If you run 2 server instances, each has its own request Map — a user could make 10 requests to server 1 and 10 to server 2 = 20 requests total. A production rate limiter would use Redis (shared across instances):

```javascript
const key = `ratelimit:${ip}`;
const count = await redis.incr(key);
if (count === 1) await redis.expire(key, 60); // set TTL on first request
if (count > 10) return res.status(429).json({ error: '...' });
```

### URL Validation

```javascript
const validator = require('validator');

if (!originalUrl || !validator.isURL(String(originalUrl), { require_protocol: true })) {
  return res.status(400).json({ error: 'Invalid URL — must include http:// or https://' });
}
```

The `validator` library provides `isURL()` which checks:
- Has a valid protocol (`http://` or `https://` with `require_protocol: true`)
- Has a valid hostname (no spaces, valid TLD, etc.)
- No malformed characters

Why not just `new URL(str)`? `new URL()` would throw on invalid URLs, which we'd need to catch. `validator.isURL()` returns a boolean — simpler control flow.

### Custom Code Validation

```javascript
if (customCode !== undefined) {
  if (customCode.length > 20 || !/^[a-zA-Z0-9-]+$/.test(customCode)) {
    return res.status(400).json({ error: '...' });
  }
}
```

The regex `/^[a-zA-Z0-9-]+$/` means:
- `^` — start of string
- `[a-zA-Z0-9-]+` — one or more alphanumeric characters or hyphens
- `$` — end of string

This prevents:
- Spaces (would break URLs)
- Special characters like `<`, `>`, `"` (XSS risk)
- `/` (would break routing)
- `.` (could be confused with file extensions)

## GET `/:shortCode`

The redirect route has two performance-critical paths:

**Fast path (cache hit):** Redis GET → TTL refresh (no await) → 302. Latency: ~1-2ms.

**Slow path (cache miss):** Redis GET (miss) → PostgreSQL SELECT → Redis SET → 302. Latency: ~5-10ms.

Both paths publish to Kafka fire-and-forget, adding ~0ms to response time (the publish completes asynchronously after the response is sent).

## GET `/api/analytics/summary`

```javascript
const [linksResult, clicksResult, dailyResult, topLinksResult] = await Promise.all([
  pool.query('SELECT COUNT(*) AS total FROM urls'),
  pool.query('SELECT COALESCE(SUM(click_count), 0) AS total FROM urls'),
  pool.query(`SELECT stat_date::text AS date, SUM(click_count) AS clicks
              FROM daily_stats WHERE stat_date >= NOW() - INTERVAL '14 days'
              GROUP BY stat_date ORDER BY stat_date`),
  pool.query(`SELECT short_code, original_url, click_count AS clicks
              FROM urls ORDER BY click_count DESC LIMIT 5`),
]);
```

`Promise.all()` runs all 4 queries in parallel — each gets its own connection from the pool and executes simultaneously. Total time = slowest query, not sum of all queries.

`COALESCE(SUM(click_count), 0)` — if there are no rows (empty table), `SUM()` returns NULL. `COALESCE` converts NULL to 0, preventing a `null` value in the response.

`stat_date::text` — PostgreSQL cast operator. Converts the DATE type to a text string in `YYYY-MM-DD` format, which JSON can serialize without a Date object.

`SUM(click_count)` in the daily_stats query — sums across all short codes for that date. This gives total clicks per day across all links.

---

# 12. React Frontend

## Application Structure

```
App.jsx (router + nav)
├── / → Home.jsx
│   ├── ShortenForm.jsx (POST /api/shorten)
│   └── LinkCard.jsx (display result + copy)
├── /dashboard → Dashboard.jsx
│   ├── StatCard.jsx × 3
│   ├── ClicksChart.jsx (BarChart)
│   └── TopLinks table
└── /analytics/:shortCode → LinkAnalytics.jsx
    ├── ClicksChart.jsx (LineChart)
    ├── TopTable.jsx (referrers)
    └── TopTable.jsx (countries)
```

## React Router v6

```javascript
// main.jsx
ReactDOM.createRoot(document.getElementById('root')).render(
  <BrowserRouter>
    <App />
  </BrowserRouter>
);

// App.jsx
<Routes>
  <Route path="/" element={<Home />} />
  <Route path="/dashboard" element={<Dashboard />} />
  <Route path="/analytics/:shortCode" element={<LinkAnalytics />} />
</Routes>
```

React Router v6 changed from `<Switch>` (v5) to `<Routes>`. Routes are matched exactly by default (no need for `exact` prop). `:shortCode` is a URL parameter, accessed via `useParams()`:

```javascript
const { shortCode } = useParams();
```

**Client-side routing** — navigation doesn't cause full page reloads. React Router intercepts link clicks and renders the appropriate component. The server only needs to serve `index.html` (Vite dev server does this automatically).

## Axios API Client

```javascript
// api/index.js
const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || 'http://localhost:4000',
});

export const shortenUrl = (data) => api.post('/api/shorten', data);
export const getAnalytics = (shortCode) => api.get(`/api/analytics/${shortCode}`);
export const getSummary = () => api.get('/api/analytics/summary');
```

`import.meta.env.VITE_API_URL` — Vite exposes environment variables prefixed with `VITE_` via `import.meta.env`. This allows the API URL to be configured per environment. In Docker, we set `VITE_API_URL=http://localhost:4000` in docker-compose.yml.

Note: this is the URL the **browser** uses. Since the browser is on your laptop (not in Docker), it connects to `localhost:4000` — which Docker maps to the server container's port 4000.

`axios.create()` — creates an axios instance with shared configuration (baseURL, headers, interceptors). All exported functions use this instance, so the baseURL is configured in one place.

## localStorage for Recent Links

```javascript
const STORAGE_KEY = 'scissor_recent_links';

const handleSuccess = (link) => {
  const updated = [link, ...recentLinks.filter((l) => l.shortCode !== link.shortCode)].slice(0, 10);
  setRecentLinks(updated);
  localStorage.setItem(STORAGE_KEY, JSON.stringify(updated));
};
```

`localStorage` is browser-persistent storage (survives page reload). We store the last 10 created links so users can see their history without authentication.

The filter `recentLinks.filter((l) => l.shortCode !== link.shortCode)` prevents duplicates — if a user shortens the same URL twice, we update the existing entry instead of duplicating.

`.slice(0, 10)` limits to 10 entries. Without this, the list would grow indefinitely.

## Recharts Components

```jsx
<ResponsiveContainer width="100%" height={240}>
  <BarChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: -16 }}>
    <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
    <XAxis dataKey="date" tick={{ fontSize: 11 }} tickLine={false} />
    <YAxis tick={{ fontSize: 11 }} tickLine={false} axisLine={false} />
    <Tooltip contentStyle={{ borderRadius: 8, border: '1px solid #e2e8f0', fontSize: 13 }} />
    <Bar dataKey="clicks" fill="#6366f1" radius={[4, 4, 0, 0]} />
  </BarChart>
</ResponsiveContainer>
```

`ResponsiveContainer` — adapts the chart to its container's width. Essential for responsive layouts.

`data` prop — array of objects: `[{ date: '2024-01-01', clicks: 120 }, ...]`

`dataKey="date"` on XAxis — tells Recharts which field to use for the X axis labels.

`dataKey="clicks"` on Bar — tells Recharts which field to use for bar heights.

`radius={[4, 4, 0, 0]}` — rounded top corners on bars (top-left, top-right, bottom-right, bottom-left).

`strokeDasharray="3 3"` — creates dashed grid lines (3px dash, 3px gap).

---

# 13. Security

## SQL Injection Prevention

All database queries use parameterized statements:
```javascript
pool.query('SELECT ... WHERE short_code = $1', [shortCode])
```

PostgreSQL's `pg` library sends the query text and parameters separately. The database treats `$1` as a placeholder, never as SQL code. Even if `shortCode` contains SQL syntax like `'; DROP TABLE urls;--`, it's safely treated as a string value.

## XSS (Cross-Site Scripting) Prevention

1. **Input validation** — custom codes are restricted to `[a-zA-Z0-9-]`. No `<`, `>`, `"` allowed.
2. **React's JSX** — React escapes all output by default. `{userValue}` in JSX cannot inject HTML. To inject raw HTML you must explicitly use `dangerouslySetInnerHTML` (which we never do).
3. **Content-Type headers** — Express returns `application/json`, not `text/html`, for API responses. Browsers won't parse JSON as HTML.

## URL Injection / SSRF Prevention

We validate that `originalUrl` is a proper URL using `validator.isURL()`. However, this doesn't prevent all SSRF scenarios. A URL like `http://169.254.169.254/latest/meta-data/` (AWS metadata endpoint) would pass validation.

A production system would additionally:
1. Blacklist private IP ranges
2. Resolve the hostname and check the resolved IP isn't private
3. Use a proxy for all outbound redirect traffic

## Rate Limiting

10 shortening requests per IP per minute prevents:
- Automated URL generation abuse
- Database flooding
- Short code space exhaustion

No rate limit on redirects (the hot path) since we want maximum performance there.

## CORS

```javascript
app.use(cors());
```

`cors()` without arguments allows all origins (`Access-Control-Allow-Origin: *`). This is fine for a public API. For a production system with an authenticated dashboard, you'd restrict to:

```javascript
app.use(cors({ origin: 'https://your-dashboard.com' }));
```

## HTTP Headers

Express sets some security headers by default, but a production system would add:

```javascript
const helmet = require('helmet');
app.use(helmet()); // Sets X-Frame-Options, X-XSS-Protection, etc.
```

---

# 14. Performance & Latency Analysis

## Redirect Latency Breakdown

**Cache hit path:**
```
Client sends GET /abc123                       0ms
Network to server (same machine)            ~0.5ms
Express routing                             ~0.2ms
Redis GET url:abc123                        ~0.3ms (in Docker network)
Redis SET url:abc123 (no await)              0ms (fire-and-forget)
Kafka publish (no await)                     0ms (fire-and-forget)
Express sends 302 response                  ~0.1ms
Network back to client                      ~0.5ms
                                          ─────────
Total client-perceived latency              ~1.5ms
```

**Cache miss path:**
```
...same as above until Redis GET...
Redis GET → miss                            ~0.3ms
PostgreSQL SELECT                           ~3-5ms (disk read with index)
Redis SET                                   ~0.3ms
Express sends 302 response                  ~0.1ms
                                          ─────────
Total client-perceived latency              ~6-8ms
```

**Cache hit rate:**

In a typical URL shortener, 80-90% of clicks are on a small fraction of links (power law distribution). A link shared on Twitter might get 10,000 clicks in an hour — all cache hits after the first. Our 86400s TTL and hit-refresh strategy ensure popular links stay in cache.

Estimated cache hit rate: >95% in steady state.

## Throughput

**Single instance Redis:** 100K+ operations/second (well within our needs).

**Single instance PostgreSQL:** ~10K-50K simple queries/second (reads with index).

**Single Kafka broker:** 1M+ messages/second (well within our needs).

**Node.js server:** At ~1ms per redirect (cache hit), one Node.js process can theoretically handle ~1000 concurrent redirects/second without blocking. In practice, ~500-1000 req/sec is realistic on a single core.

## Memory Usage

**Redis:** 100K URLs × ~200 bytes per URL ≈ 20MB. Plus Redis overhead ≈ ~30MB total. Our 256MB limit is generous.

**Kafka:** Messages are written to disk (log files). Message retention defaults to 7 days. At 100K clicks/day × 200 bytes = 20MB/day × 7 days = 140MB.

**PostgreSQL:** With `click_events` growing at 100K rows/day, after 1 year ≈ 36.5M rows. At ~200 bytes/row ≈ 7GB. This is when you'd want to implement partitioning by month (PostgreSQL table partitioning).

---

# 15. Failure Modes & Resilience

## What Happens If Redis Goes Down?

The redirect controller has a fallback:

```javascript
let originalUrl = await getUrl(shortCode);
if (originalUrl) {
  // use cache
} else {
  // fall back to DB
  const result = await pool.query('SELECT original_url ...');
  // ...
}
```

If Redis is down, `getUrl()` will throw an error (ioredis connection error). This error propagates to Express's error handler, returning a 500. Redirects would fail entirely.

**Production fix:** Wrap the cache call in try-catch:

```javascript
let originalUrl = null;
try {
  originalUrl = await getUrl(shortCode);
} catch (err) {
  console.error('Redis unavailable, falling back to DB');
}
// always fall through to DB if cache returns null
```

## What Happens If Kafka Goes Down?

The `publishClickEvent` function is fire-and-forget with a `.catch()`. If Kafka is down:
1. The `producer.send()` promise rejects
2. `.catch()` logs the error
3. The redirect still completes successfully
4. That click event is **lost** (not recorded)

KafkaJS's built-in retry will attempt to reconnect in the background. Once Kafka comes back, new events are published. Events that were attempted while Kafka was down are gone.

**Production fix:** Buffer events locally (in-memory or Redis) and replay when Kafka reconnects. Or use KafkaJS's `maxInFlightRequests` and `retry` settings more aggressively.

## What Happens If PostgreSQL Goes Down?

- Cache hits still work (Redis is independent of Postgres)
- Cache misses → DB query fails → 500 error
- URL shortening fails (insert fails)
- Analytics queries fail

With `restart: on-failure` in docker-compose, the server and consumer restart if they crash due to DB connection failure.

## Consumer Failures

The consumer is stateless — it reads from Kafka and writes to PostgreSQL. If it crashes, it restarts (docker-compose `restart: on-failure`) and resumes from the last committed offset. No clicks are lost from Kafka (Kafka retains messages for 7 days by default).

If the consumer processes a message but crashes before committing the offset, it will reprocess the same message after restart. This results in duplicate rows in `click_events` and double-incremented counters in `daily_stats` and `urls.click_count`.

**Production fix:** Make the consumer idempotent by checking if a click_event already exists before inserting:

```sql
INSERT INTO click_events (...)
ON CONFLICT (short_code, clicked_at, ip_address) DO NOTHING;
```

Or use a Kafka message ID and store it to detect duplicates.

## Health Check Dependency Chain

```
postgres healthy
    ↓
redis healthy      (independent)
    ↓
zookeeper healthy
    ↓
kafka healthy
    ↓
server starts
    ↓
consumer starts
```

If any layer in this chain fails, everything downstream waits. Docker's `restart: on-failure` catches cases where services start but then crash.

---

# 16. Scaling

## Vertical Scaling (Scale Up)

The simplest approach: give the server more CPU and RAM. Node.js is single-threaded, so adding more cores doesn't help a single Node.js process. But you can use:

```javascript
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

if (cluster.isMaster) {
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork(); // spawn one worker per CPU core
  }
} else {
  // worker code — your Express app
  require('./app');
}
```

This gives you `N` Node.js processes sharing the same port via the OS kernel's load balancing.

## Horizontal Scaling (Scale Out)

Run multiple server instances:

```
Load Balancer (nginx/HAProxy)
       │
       ├──▶ Server Instance 1 (MACHINE_ID=1)
       ├──▶ Server Instance 2 (MACHINE_ID=2)
       └──▶ Server Instance 3 (MACHINE_ID=3)
```

Key requirements for horizontal scaling:

1. **Snowflake MACHINE_ID** — each instance must have a unique MACHINE_ID (0-1023). Already designed for this.

2. **Shared Redis** — all instances must hit the same Redis. Already designed for this (Redis is a separate service, not in-process).

3. **Shared Kafka** — all instances produce to the same Kafka topic. Already designed for this.

4. **Rate limiter** — our in-memory rate limiter becomes per-instance. Switch to Redis-based rate limiting (as described in Security section).

5. **Session state** — we have no server-side sessions (stateless API). Already designed for this.

## Redis Scaling

**Single Redis for ~100K clicks/day:** ✅ Fine.

**For 10M+ clicks/day:**

1. **Redis Cluster** — shards data across multiple Redis nodes using consistent hashing. `ioredis` supports cluster mode:
   ```javascript
   const Redis = require('ioredis');
   const cluster = new Redis.Cluster([
     { host: 'redis-1', port: 6379 },
     { host: 'redis-2', port: 6379 },
     { host: 'redis-3', port: 6379 },
   ]);
   ```

2. **Redis Sentinel** — provides high availability (automatic failover) without sharding.

3. **Read replicas** — route reads to replicas, writes to primary.

## Kafka Scaling

**Increase partitions:** More partitions = more consumer parallelism. But you can't decrease partitions without recreating the topic.

**Multiple brokers:** Add more Kafka brokers for higher throughput and replication factor > 1.

**Consumer instances:** Scale the consumer service horizontally. Each instance joins the same consumer group and gets assigned partitions.

```yaml
# docker-compose.yml
consumer:
  deploy:
    replicas: 3  # 3 consumer instances, each handles 1 partition
```

## PostgreSQL Scaling

**Read replicas:** Route analytics SELECT queries to a read replica. Write queries (INSERT into click_events, UPDATE urls) go to primary.

```javascript
const readPool = new Pool({ connectionString: process.env.DATABASE_REPLICA_URL });
const writePool = new Pool({ connectionString: process.env.DATABASE_URL });
```

**Table partitioning:** Partition `click_events` by month. Old months can be archived or dropped:

```sql
CREATE TABLE click_events_2024_01 PARTITION OF click_events
  FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

**Connection pooling:** Use PgBouncer as a connection pooler. PostgreSQL has a limit of ~100-500 connections. If you have 10 Node.js instances each with a pool of 10, you're using 100 connections. PgBouncer multiplexes thousands of application connections onto a few database connections.

---

# 17. Code Walkthrough

## File: `server/src/config/db.js`

```javascript
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

pool.on('error', (err) => console.error('PostgreSQL pool error:', err));

module.exports = pool;
```

`Pool` — manages a collection of database connections. Instead of creating a new connection for every query (expensive: TCP handshake + PostgreSQL auth), the pool reuses connections.

Default pool size: 10 connections. Configurable via `{ connectionString: ..., max: 20 }`.

`pool.on('error')` — handles errors on idle connections. Without this listener, an error on an idle connection (e.g., PostgreSQL restarted) would be an unhandled error and crash Node.js.

`module.exports = pool` — exports a singleton. Every `require('./config/db')` in the same process gets the same Pool instance. The pool is shared across all route handlers.

## File: `server/src/config/redis.js`

```javascript
const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL, {
  retryStrategy: (times) => Math.min(times * 200, 5000),
  maxRetriesPerRequest: null,
  lazyConnect: false,
});
```

`retryStrategy: (times) => ...` — a function that returns the number of milliseconds to wait before reconnecting. `times` is how many retries have been attempted. `Math.min(times * 200, 5000)` gives linear backoff capped at 5 seconds: 200ms, 400ms, 600ms, ... 5000ms, 5000ms, 5000ms...

If you return `null` from retryStrategy, ioredis stops retrying (throws error).

`maxRetriesPerRequest: null` — commands queued while disconnected retry indefinitely. Setting to a number (e.g., `3`) would cause commands to fail with an error after 3 retries.

## File: `server/src/middleware/rateLimiter.js`

```javascript
const requests = new Map();

setInterval(() => {
  const cutoff = Date.now() - 60000;
  for (const [ip, times] of requests.entries()) {
    const fresh = times.filter((t) => t > cutoff);
    if (fresh.length === 0) requests.delete(ip);
    else requests.set(ip, fresh);
  }
}, 60000);
```

The `setInterval` cleanup runs every 60 seconds. For each IP in the map, filter out timestamps older than 60 seconds. If an IP has no recent requests, delete it from the map entirely. This prevents the Map from growing indefinitely as unique IPs visit the server.

Note: this `setInterval` runs as long as the Node.js process runs. In test environments, you'd want to clear it with `clearInterval` to prevent the test from hanging.

## File: `server/src/controllers/redirectController.js`

```javascript
publishClickEvent({
  shortCode,
  clickedAt: new Date().toISOString(),
  ipAddress: req.ip || req.socket.remoteAddress,
  userAgent: req.headers['user-agent'] || '',
  referrer: req.headers['referer'] || '',
});
```

`req.ip` — with `trust proxy: 1` set, this returns the client's IP from `X-Forwarded-For`. Without trust proxy, it returns the socket's remote address (the proxy's IP).

`req.headers['user-agent']` — the User-Agent string. Note: headers in Express are lowercase regardless of how the client sent them.

`req.headers['referer']` — note the intentional misspelling. The HTTP spec misspelled "referrer" as "referer" in the original 1996 spec (RFC 1945). The header name `Referer` is the standard (one 'r'). This typo is now enshrined in the HTTP spec forever.

## File: `consumer/src/handlers/clickHandler.js`

```javascript
const statDate = clickedAt.split('T')[0]; // 'YYYY-MM-DD'
```

`clickedAt` is an ISO 8601 string: `"2024-01-15T14:30:00.000Z"`. `.split('T')[0]` extracts the date part `"2024-01-15"`. This is how we bucket clicks into days for `daily_stats`.

A more robust approach: use the database to extract the date:
```sql
DATE(clicked_at AT TIME ZONE 'UTC')
```

But string splitting is fine since we always use UTC ISO strings.

## File: `client/src/pages/Home.jsx`

```javascript
useEffect(() => {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) setRecentLinks(JSON.parse(stored));
  } catch {
    // ignore parse errors
  }
}, []);
```

The empty dependency array `[]` means this effect runs once, on component mount (equivalent to `componentDidMount` in class components). We load recent links from localStorage on page load.

The `try/catch` around `JSON.parse` is defensive: if localStorage contains malformed JSON (e.g., someone manually edited it), it won't crash the app.

## File: `client/src/components/ClicksChart.jsx`

```jsx
{type === 'bar' ? (
  <BarChart data={data} ...>
    ...
    <Bar dataKey="clicks" fill="#6366f1" radius={[4, 4, 0, 0]} />
  </BarChart>
) : (
  <LineChart data={data} ...>
    ...
    <Line type="monotone" dataKey="clicks" stroke="#6366f1" ... />
  </LineChart>
)}
```

`type="monotone"` on the Line — controls the interpolation between data points. `"monotone"` uses monotone cubic spline interpolation, which creates smooth curves that don't overshoot the data points (unlike "natural" or "cardinal" which can create artifacts). It looks smooth without being misleading about the data.

Alternatives: `"linear"` (straight lines between points), `"step"` (staircase).

---

# 18. Interview Q&A

## System Design Questions

**Q: Why did you choose Redis for caching instead of Memcached?**

A: Both are in-memory key-value stores. Redis has several advantages for this use case:
1. **Persistence** — Redis can write to disk (RDB snapshots, AOF logs). If Redis restarts, cache survives. Memcached is purely in-memory — restart clears all data.
2. **Data structures** — Redis supports strings, hashes, lists, sets, sorted sets. We only use strings here, but Redis sets would be useful for tracking unique visitors per link.
3. **Atomic operations** — `INCR`, `EXPIRE`, `SET EX` are atomic. `ioredis` is the standard Node.js client for Redis and has excellent cluster support.

**Q: What's the difference between your daily_stats table and querying click_events with GROUP BY?**

A: The GROUP BY approach on click_events is O(n) — it scans all relevant rows. As click_events grows to millions of rows, this gets progressively slower. The daily_stats table is pre-aggregated: we update it on every click event. Dashboard queries hit the (short_code, stat_date) primary key index — O(1) lookup. This is the same trade-off made by OLAP systems like BigQuery, Druid, and ClickHouse: write more work upfront to make reads fast.

**Q: What happens if you receive the same click event twice (Kafka "at least once" delivery)?**

A: We'd get a duplicate row in click_events, and daily_stats.click_count and urls.click_count would be double-incremented. Our current system doesn't handle this. The fix: use a deduplication key (e.g., a message ID or hash of shortCode+clickedAt+ipAddress) and use `INSERT ... ON CONFLICT DO NOTHING` in click_events. For daily_stats, the upsert already handles concurrent increments correctly.

**Q: How would you handle URL shortener abuse (phishing links, malware)?**

A: Multiple layers:
1. **Blocklist** — check the original URL against a known malware/phishing database (Google Safe Browsing API).
2. **Content scanning** — fetch the URL's content and scan for malicious patterns.
3. **User reporting** — allow users to report links; route reports to human review.
4. **Domain blacklisting** — ban known phishing domains.
5. **Rate limiting** — limit link creation per account/IP.
6. **Age verification** — new URLs can't be redirected for 60 seconds (gives time to scan).

**Q: How would you add user authentication to this system?**

A: 
1. Add a `users` table (id, email, password_hash, created_at)
2. Add `user_id BIGINT REFERENCES users(id)` to the `urls` table
3. Use JWT (JSON Web Tokens) for stateless authentication
4. On sign-in: verify password with bcrypt, return signed JWT
5. On API requests: verify JWT in `Authorization: Bearer <token>` header
6. Dashboard shows only the authenticated user's links

**Q: Your Snowflake IDs are only guaranteed unique per process. What if two server instances generate the same ID?**

A: The MACHINE_ID prevents this. Each server instance is given a unique MACHINE_ID (0-1023). The ID includes `MACHINE_ID << 12` in its 64-bit composition. Two instances with different MACHINE_IDs will never produce the same ID, even if they generate an ID at the same millisecond with the same sequence number.

If we accidentally gave two servers the same MACHINE_ID, the PostgreSQL `UNIQUE` constraint on `short_code` would catch the collision and return a 409 error, preventing data corruption.

**Q: How does the Kafka consumer know where to resume after a restart?**

A: Kafka tracks consumer progress using offsets stored in the `__consumer_offsets` topic (an internal Kafka topic). Each consumer group has its own set of offsets per partition. When the consumer restarts with the same `groupId`, it reads the last committed offset and resumes from there. KafkaJS auto-commits offsets periodically (default: every 5 seconds in autoCommit mode).

**Q: Why 3 Kafka partitions? What's the trade-off of more partitions?**

A: 3 partitions = maximum 3 consumer instances running in parallel. More partitions = more parallelism, but with costs:
- More file handles on the broker (each partition = files on disk)
- More replication traffic
- Longer rebalance time when consumers join/leave
- Each partition has a minimum leader election time

A rule of thumb: start with `max_consumers * 2` partitions. We chose 3 because we anticipate at most 1-3 consumer instances.

**Q: What is Zookeeper doing in your stack and could you remove it?**

A: Zookeeper is the coordination service Kafka depends on for:
1. Electing which broker is the partition leader
2. Storing topic configurations and partition assignments
3. Detecting broker failures (heartbeats)

Yes, we could remove it with **KRaft mode** (Kafka Raft metadata mode), available since Kafka 3.3. KRaft embeds the coordination logic directly into Kafka brokers using the Raft consensus algorithm (same algorithm used by etcd, which powers Kubernetes). We'd need a newer Confluent image that supports KRaft. The benefit: simpler stack (one less service to manage).

**Q: How would you implement custom domains (bit.ly/mycompany)?**

A: 
1. Add a `domain` field to the `urls` table
2. Accept a `customDomain` parameter in POST /api/shorten
3. Store the DNS configuration (user proves domain ownership via DNS TXT record)
4. When a redirect request arrives, check the `Host` header to determine which domain's links to look up
5. The Redis cache key becomes `url:<domain>:<shortCode>` instead of just `url:<shortCode>`

**Q: What's the maximum number of unique short URLs this system can generate?**

A: Our Snowflake IDs in Base62 produce 7-10 character codes. At 7 characters: 62^7 = 3.5 trillion unique codes. At 10 characters: 62^10 = 839 trillion. PostgreSQL BIGSERIAL (8 bytes) supports 9.2 × 10^18 rows. We'll never run out of IDs.

**Q: How would you implement link expiry cleanup?**

A: Currently, expired links are detected at redirect time and return 410. But they remain in PostgreSQL and Redis forever. A cleanup job:
1. A scheduled job (cron, or a Kafka consumer) runs daily
2. Queries `SELECT short_code FROM urls WHERE expires_at < NOW()`
3. For each expired code: calls `cacheService.invalidate(shortCode)` and optionally deletes from `urls`

Whether to delete from `urls` is a business decision — you might want to keep historical data but just not redirect.

---

## Code & Implementation Questions

**Q: Why do you use BigInt for the Snowflake ID calculation?**

A: JavaScript numbers are 64-bit IEEE 754 doubles, which can represent integers exactly up to 2^53 (about 9 quadrillion). Our Snowflake IDs use 64 bits. The timestamp alone (41 bits) combined with machine ID (10 bits) and sequence (12 bits) produces numbers larger than 2^53. Bitwise operations (`<<`, `|`) in JavaScript operate on 32-bit integers, not 64-bit. BigInt (the `n` suffix) gives us true 64-bit integer arithmetic with correct bitwise operations.

**Q: What is `trust proxy` and why do you set it to 1?**

A: Express's `req.ip` normally returns the IP of the direct TCP connection. If your server is behind a proxy (nginx, load balancer), the direct connection is from the proxy. The real client IP is in the `X-Forwarded-For` header added by the proxy. `trust proxy: 1` tells Express to trust the first hop proxy and extract `req.ip` from `X-Forwarded-For`. Without this, the rate limiter would rate-limit the proxy (same IP for all clients) instead of individual users.

**Q: Why does the /api/analytics/summary route need to be registered before /:shortCode?**

A: In the analytics router, both `/summary` and `/:shortCode` match a single path segment. Express matches routes in registration order. If `/:shortCode` were registered first, a request to `/api/analytics/summary` would be caught by `/:shortCode` with `shortCode = 'summary'`, resulting in a DB query for a non-existent short code. By registering `/summary` first, Express matches it specifically before falling through to the parameter route.

**Q: Why don't you await the Redis TTL refresh in the redirect handler?**

A: The TTL refresh (`setUrl(shortCode, originalUrl)`) is not critical to the redirect. The user is already going to be redirected regardless of whether the TTL refresh succeeds. By not awaiting it, we save ~0.3ms on every cache-hit redirect. If the refresh fails (Redis temporarily unavailable), the link still works — it just might expire slightly sooner than expected. This is a latency-vs-reliability trade-off in favor of latency.

**Q: Explain the ON CONFLICT ... DO UPDATE syntax in your daily_stats upsert.**

A: This is PostgreSQL's "upsert" syntax. `ON CONFLICT (short_code, stat_date)` specifies the conflict target — if an INSERT would violate the primary key constraint on `(short_code, stat_date)`, instead of failing, execute the `DO UPDATE`. `SET click_count = daily_stats.click_count + 1` increments the existing value. This is atomic — no separate SELECT needed, no race condition between check and update. It's equivalent to:
```sql
-- What PostgreSQL does internally (conceptually):
IF EXISTS (SELECT 1 FROM daily_stats WHERE short_code=? AND stat_date=?) THEN
  UPDATE daily_stats SET click_count = click_count + 1 WHERE ...;
ELSE
  INSERT INTO daily_stats VALUES (..., 1);
END IF;
-- But atomically, with no race condition
```

**Q: What happens if two consumer instances process the same click event simultaneously?**

A: Kafka's partition assignment prevents this. Each partition is assigned to exactly one consumer instance within a consumer group at any time. If you have 3 partitions and 2 consumers, one consumer gets 2 partitions and the other gets 1. No two consumers ever read from the same partition simultaneously within a group.

**Q: Why does the React client use localStorage instead of the server for "recent links"?**

A: There's no user authentication in this system. Without knowing who the user is, we can't store their links server-side. localStorage is a browser-side, per-domain persistent store. It works without a backend and survives page refreshes. The downside: it's device-specific (you won't see your links on a different browser or after clearing browser data).

**Q: What is `Promise.all()` and why do you use it in the analytics controller?**

A: `Promise.all(array)` takes an array of promises and returns a single promise that resolves when ALL input promises resolve, with an array of their results. If any promise rejects, `Promise.all` immediately rejects. It runs all promises simultaneously (not sequentially). In our analytics summary, we run 4 independent PostgreSQL queries simultaneously. Total time = max(query1_time, query2_time, query3_time, query4_time) instead of their sum.

**Q: How does Vite's dev server work with the React app in Docker?**

A: Vite's dev server (started with `npm run dev`) serves JavaScript as native ES modules — the browser fetches each file individually. In Docker, the Vite server runs in a container with `host: true` (listening on 0.0.0.0), mapped to host port 3000. When you open `localhost:3000`, your browser connects to Vite, which serves the React app's files. The React app then makes API calls to `localhost:4000` (the server container's exposed port). All communication between React and the API happens browser-to-server, not container-to-container.

**Q: Explain CORS and why you need it in this project.**

A: CORS (Cross-Origin Resource Sharing) is a browser security policy. When JavaScript on `localhost:3000` makes an HTTP request to `localhost:4000`, the browser sees this as a cross-origin request (different port = different origin). By default, browsers block cross-origin requests. CORS headers (`Access-Control-Allow-Origin: *`) tell the browser it's allowed. This is enforced by the BROWSER, not the server — the server receives all requests regardless, but the browser hides the response from JavaScript if CORS headers are missing. The `cors()` Express middleware adds these headers.

---

## Architecture & Trade-offs Questions

**Q: If you could redo this architecture, what would you change?**

A:
1. **Use Redis Streams instead of Kafka** — for 100K clicks/day, Kafka is overkill. Redis Streams have persistence, consumer groups, and acknowledgment semantics, with far simpler operational overhead.
2. **Use Fastify instead of Express** — Fastify is 2x faster, has built-in validation (no need for `validator` package), and TypeScript support.
3. **KRaft mode Kafka** — remove Zookeeper for a simpler stack.
4. **Connection pooling with PgBouncer** — for high-concurrency scenarios.
5. **Dedicated analytics database** — ClickHouse or TimescaleDB instead of PostgreSQL for the analytics tables, since those are time-series workloads.

**Q: How would your design change for 1 billion clicks/day?**

A:
1. **CDN for redirects** — cache popular short codes at CDN edge nodes. No server hit at all for cache hits.
2. **Kafka partitions: 100+** — to support 100 consumer instances.
3. **PostgreSQL sharding** — shard the `click_events` table by hash(short_code) across multiple databases.
4. **Redis Cluster** — 10+ nodes, consistent hashing.
5. **Pre-sorted short code cache** — use sorted sets in Redis to serve "top links" without hitting PostgreSQL.
6. **Columnar analytics database** — replace daily_stats with ClickHouse (100x faster for analytical queries).
7. **Read replicas** — 5-10 Postgres read replicas for analytics queries.

**Q: What is the CAP theorem and how does it apply here?**

A: CAP theorem states that a distributed system can guarantee at most two of:
- **C**onsistency — every read returns the most recent write
- **A**vailability — every request gets a response (not an error)
- **P**artition tolerance — the system works even if the network splits

Our system:
- **PostgreSQL** — CP (consistent + partition tolerant). In a network partition, PostgreSQL will refuse writes rather than risk inconsistency.
- **Redis** — AP (available + partition tolerant). If network splits, Redis might serve stale data from one side of the split.
- **Kafka** — uses leader election via Zookeeper — CP for writes (the leader must acknowledge), but if the leader is partitioned, writes temporarily fail until a new leader is elected.

For a URL shortener, eventual consistency for click counts is acceptable (showing 999 instead of 1000 clicks for a moment is fine). But redirect correctness (serving the right URL) requires stronger consistency.

**Q: What does "idempotent" mean and where does it matter in this system?**

A: An operation is idempotent if running it multiple times produces the same result as running it once.

- **GET requests** — naturally idempotent. Refreshing the analytics page gives the same data.
- **POST /api/shorten** — NOT idempotent. Submitting the same form twice creates two short codes (unless `customCode` is used, in which case the second attempt returns 409 Conflict).
- **Kafka consumer writes** — currently NOT idempotent. If a click event is processed twice (at-least-once delivery), click counts are doubled. Making it idempotent: use `INSERT ... ON CONFLICT DO NOTHING` with a unique constraint on `(short_code, clicked_at, ip_address, user_agent)`.
- **Database migrations** — `CREATE TABLE IF NOT EXISTS` is idempotent. Running migrate.js multiple times on the same database is safe.

---

## Behavioral/Explanation Questions

**Q: Walk me through what happens when I paste a URL and click Shorten.**

A:
1. React's ShortenForm `handleSubmit` calls `shortenUrl({ originalUrl: '...' })`
2. Axios sends `POST http://localhost:4000/api/shorten` with JSON body
3. Browser sends CORS preflight OPTIONS request first; server responds with CORS headers
4. Axios sends the actual POST
5. Express routes it to `routes/shorten.js` → `shortenController.shorten`
6. Rate limiter checks: this IP has < 10 requests in the last minute ✓
7. `validator.isURL()` validates the URL ✓
8. `generateId()` creates a Snowflake ID: takes current timestamp, adds machine ID bits and sequence bits, converts to Base62 → "3xK9mP"
9. `pool.query('INSERT INTO urls ...')` writes to PostgreSQL
10. `setUrl('3xK9mP', 'https://...')` writes to Redis with 24h TTL
11. Response 201: `{ shortUrl: 'http://localhost:4000/3xK9mP', shortCode: '3xK9mP', ... }`
12. Axios resolves, `onSuccess(data)` is called in React
13. State updates: `recentLinks = [{ shortCode: '3xK9mP', ... }, ...prevLinks]`
14. `localStorage.setItem(...)` persists the link
15. React re-renders, showing `<LinkCard>` with the short URL and a Copy button

**Q: Walk me through what happens when someone clicks the short URL.**

A:
1. Browser opens `http://localhost:4000/3xK9mP`
2. HTTP GET request to Express server
3. Express matches the `/:shortCode` route (after /api/* routes don't match)
4. `redirectController.redirect` is called
5. `getUrl('3xK9mP')` → Redis GET `url:3xK9mP`
   - Cache HIT: got the original URL
6. `setUrl('3xK9mP', originalUrl)` → Redis SET with fresh 86400s TTL (no await)
7. `publishClickEvent({ shortCode: '3xK9mP', clickedAt: now, ipAddress: '1.2.3.4', ... })` → Kafka (no await)
8. `res.redirect(302, 'https://original-long-url.com')` sent immediately
9. Browser follows the 302 redirect to the original URL
10. [Asynchronously] Kafka consumer receives the click event
11. Calls ip-api.com for '1.2.3.4' → "India" (or from ipCache if seen before)
12. PostgreSQL transaction:
    - INSERT into click_events
    - UPSERT into daily_stats (2024-01-15 count++)
    - UPDATE urls SET click_count = click_count + 1
    - COMMIT

**Q: How would you debug if click counts in the dashboard are wrong?**

A:
1. Check `urls.click_count` directly: `SELECT short_code, click_count FROM urls`
2. Cross-check with raw events: `SELECT COUNT(*) FROM click_events WHERE short_code = 'x'`
3. Check daily_stats: `SELECT SUM(click_count) FROM daily_stats WHERE short_code = 'x'`
4. Check Kafka consumer logs — are errors occurring? Is it processing messages?
5. Check if the consumer is running at all: `docker-compose logs consumer`
6. Check Kafka consumer group lag: `kafka-consumer-groups --bootstrap-server kafka:29092 --group analytics-consumer-group --describe` — shows if messages are piling up unprocessed
7. Check if the click_events table is being populated

---

## Quick Revision — Key Numbers to Remember

| Metric | Value |
|---|---|
| Redis cache TTL | 86400 seconds (24 hours) |
| Redis max memory | 256MB |
| Kafka partitions | 3 |
| Kafka replication factor | 1 (dev) / 3 (prod) |
| Rate limit | 10 shortens/IP/minute |
| Short code max length | 12 chars (VARCHAR(12)) |
| IP address max length | 45 chars (IPv4-mapped IPv6) |
| Snowflake machine IDs | 1024 (10 bits) |
| Snowflake IDs/ms/machine | 4096 (12 bits) |
| Snowflake epoch | November 2023 (1700000000000ms) |
| Snowflake valid until | ~2092 (41 bits × timestamp) |
| Connection pool default | 10 connections |
| PostgreSQL max connections | ~100-500 (depends on config) |
| Kafka consumer group | analytics-consumer-group |
| API server port | 4000 |
| React client port | 3000 |
| PostgreSQL port | 5432 |
| Redis port | 6379 |
| Kafka external port | 9092 |
| Kafka internal port | 29092 |
| Zookeeper port | 2181 |

---

## Conceptual Terms — Glossary

**Write-through cache** — Write to both cache AND database simultaneously on every write.

**Cache aside** — Read from cache; on miss, read from DB and populate cache.

**Write-behind cache** — Write to cache first, asynchronously flush to DB. Faster writes, risk of data loss.

**LRU (Least Recently Used)** — Eviction policy: remove the item that hasn't been accessed for the longest time.

**TTL (Time to Live)** — Expiry duration for a cache entry.

**Connection pooling** — Reuse database connections instead of creating new ones per query.

**ACID** — Atomicity, Consistency, Isolation, Durability: guarantees of relational databases.

**Idempotent** — An operation that produces the same result when run multiple times.

**Partition** — A horizontal subdivision of a Kafka topic; unit of parallelism.

**Consumer group** — A group of Kafka consumers that share partition assignment.

**Offset** — The position of a message within a Kafka partition. Like a bookmark.

**Dead Letter Queue (DLQ)** — A Kafka topic where failed messages are sent for inspection.

**SSRF (Server-Side Request Forgery)** — An attack where the server is tricked into making requests to internal resources.

**Base62** — Number encoding using digits 0-9, lowercase a-z, uppercase A-Z (62 chars total).

**Snowflake ID** — A 64-bit ID scheme (timestamp + machine ID + sequence) developed by Twitter.

**Fire-and-forget** — Sending a message without waiting for acknowledgment. Higher throughput, possible message loss.

**Denormalization** — Storing redundant computed data (like `click_count` on `urls`) to avoid expensive JOINs or aggregations at query time.

**Pre-aggregation** — Computing aggregate values (like daily click counts) at write time rather than query time.

**Replication factor** — How many copies of each Kafka partition exist across brokers. Higher = more fault tolerance.

**Leader election** — The process by which distributed nodes choose one node to coordinate writes. Used by Kafka (via Zookeeper) for partition leaders.

**At-least-once delivery** — A Kafka guarantee: every message will be delivered to the consumer at least once. Duplicates are possible.

**Exactly-once delivery** — A stronger Kafka guarantee: every message is delivered exactly once. Requires transactions and idempotent producers.

---

*This document covers every system design concept, every technology choice rationale, and every line of code explanation you would need for a thorough technical interview on this project. Good luck!*
