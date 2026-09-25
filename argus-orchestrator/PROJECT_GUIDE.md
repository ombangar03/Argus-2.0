# Argus 2.0: Master Project Guide & Code Walkthrough

This document is the **definitive, step-by-step master guide** for the entire Argus 2.0 system. It covers both microservices — `argus-orchestrator` and `argus-connector` — documenting every architectural decision, every directory, every file, every bug encountered, and every fix applied.

---

## Table of Contents
1. [System Architecture: What Are We Building?](#1-system-architecture-what-are-we-building)
2. [The Request State Lifecycle](#2-the-request-state-lifecycle)
3. [Repository Directory Structures](#3-repository-directory-structures)
4. [argus-orchestrator: File-by-File Walkthrough](#4-argus-orchestrator-file-by-file-walkthrough)
   - [Config: `app/core/config.py`](#file-1-appcoreconfigpy)
   - [Logging: `app/core/logging.py`](#file-2-appcoreloggingpy)
   - [MongoDB Client: `app/platform/mongodb.py`](#file-3-appplatformmongodbpy)
   - [Redis Client: `app/platform/redis.py`](#file-4-appplatformredispy)
   - [FastAPI & Lifespan: `app/main.py`](#file-5-appmainpy)
   - [Health API: `app/api/health.py`](#file-6-appapihealthpy)
   - [Seed Script: `scripts/add_rss_request.py`](#file-7-scriptsadd_rss_requestpy)
   - [Outbound Schema: `app/schemas/request.py`](#file-8-appschemasrequestpy)
   - [Outbound Service: `app/services/request_service.py`](#file-9-appservicesrequest_servicepy)
   - [Outbound Worker: `app/workers/request_worker.py`](#file-10-appworkersrequest_workerpy)
   - [Inbound Schema: `app/schemas/result.py`](#file-11-appschemasresultpy)
   - [Inbound Service: `app/services/result_service.py`](#file-12-appservicesresult_servicepy)
   - [Inbound Worker: `app/workers/result_worker.py`](#file-13-appworkersresult_workerpy)
   - [News Query Schema: `app/schemas/query.py`](#file-14-appschemasquerypy)
   - [Bing URL Builder: `app/services/bing_service.py`](#file-15-appservicesbing_servicepy)
   - [Seed News Queries: `scripts/seed_news_query.py`](#file-16-scriptsseed_news_querypy)
   - [Bing Test Script: `app/test/test_bing.py`](#file-17-apptesttest_bingpy)
5. [argus-connector: File-by-File Walkthrough](#5-argus-connector-file-by-file-walkthrough)
   - [Config: `app/core/config.py`](#connector-file-1-appcoreconfigpy)
   - [Logging: `app/core/logging.py`](#connector-file-2-appcoreloggingpy)
   - [Redis Client: `app/platform/redis.py`](#connector-file-3-appplatformredispy)
   - [Connector Schema: `app/schemas/connector.py`](#connector-file-4-appschemasconnectorpy)
   - [Base Connector Abstraction: `app/connectors/base.py`](#connector-file-5-appconnectorsbasepy)
   - [Connector Registry: `app/connectors/registry.py`](#connector-file-6-appconnectorsregistrypy)
   - [RSS Connector: `app/connectors/rss/connector.py`](#connector-file-7-appconnectorsrssconnectorpy)
   - [RSS Data Model: `app/connectors/rss/model.py`](#connector-file-8-appconnectorsrssmodelpy)
   - [RSS Parser: `app/connectors/rss/parser.py`](#connector-file-9-appconnectorsrssparserpy)
   - [Generic Connector Worker: `app/workers/connector_worker.py`](#connector-file-10-appworkersconnector_workerpy)
   - [FastAPI Entrypoint: `app/main.py`](#connector-file-11-appmainpy)
   - [Connector Test Suite: `app/test/`](#connector-file-12-apptest)
6. [End-to-End Pipeline: How to Run the Full System](#6-end-to-end-pipeline-how-to-run-the-full-system)
7. [Bugs Encountered & Fixes Applied](#7-bugs-encountered--fixes-applied)
8. [Current Roadmap & Project Progress](#8-current-roadmap--project-progress)

---

## 1. System Architecture: What Are We Building?

Argus 2.0 is an **event-driven, decoupled web intelligence and content ingestion system**.

Instead of building a monolithic script that downloads feeds, parses articles, and saves them in one place, the system is split into two independent microservices communicating exclusively through **Redis Streams**:

```text
┌────────────────────────────────────────────────────────────────────────┐
│                    ARGUS ORCHESTRATOR  (Port 8000)                     │
│                                                                        │
│  scripts/add_rss_request.py                                            │
│         │ (1. Insert 'pending' task into MongoDB)                      │
│         ▼                                                              │
│  [MongoDB: rss_requests]                                               │
│         │ (2. Atomic Claim: status -> 'processing')                    │
│         ▼                                                              │
│  [request_worker.py] --> [request_service.py: dispatch_request]        │
│                                  │                                     │
│                                  │ (3. xadd: Generic ConnectorRequest) │
│                                  │     {request_id, source,            │
│                                  │      source_type, source_url,       │
│                                  │      metadata: "{...}"}             │
└──────────────────────────────────┼─────────────────────────────────────┘
                                   │
                                   ▼
          ┌──────────────────────────────────────────────────┐
          │  Redis Stream: 'queue:connector:rss'             │
          └──────────────────────────────────────────────────┘
                                   │
                                   ▼ (4. Pull via xreadgroup)
┌────────────────────────────────────────────────────────────────────────┐
│                    ARGUS CONNECTOR  (Port 8001)                        │
│                                                                        │
│  [connector_worker.py]                                                 │
│         │                                                              │
│         │ (5. get_connector(request.source_type))                      │
│         ▼                                                              │
│  [registry.py] ──> [rss/connector.py: RSSConnector]                   │
│                           │                                            │
│                           │ (6. httpx fetches feed XML asynchronously) │
│                           │ (7. feedparser extracts articles)          │
│                           ▼                                            │
│                    [CrawlResult payload]                               │
│                           │                                            │
│                           │ (8. xadd: Publish CrawlResult JSON)        │
└───────────────────────────┼────────────────────────────────────────────┘
                            │
                            ▼
          ┌──────────────────────────────────────────────────┐
          │  Redis Stream: 'connector:rss:results'           │
          └──────────────────────────────────────────────────┘
                            │
                            ▼ (9. xreadgroup via consumer group)
┌────────────────────────────────────────────────────────────────────────┐
│                    ARGUS ORCHESTRATOR (continued)                      │
│                                                                        │
│  [result_worker.py] --> [result_service.py]                            │
│                                  │                                     │
│                                  ├──> [MongoDB: rss_items] (Articles)  │
│                                  │    (Bulk upsert, zero duplicates)   │
│                                  │                                     │
│                                  └──> [MongoDB: rss_requests]          │
│                                       (status -> 'completed')          │
└────────────────────────────────────────────────────────────────────────┘
```

### Why This Architecture?
1. **Generic Multi-Connector Extensibility:** The Orchestrator and Connector no longer speak an RSS-only dialect. Tasks are dispatched as generic `ConnectorRequest` objects specifying `source`, `source_type`, `source_url`, and custom `metadata`. Adding a new source (e.g. NSE, BSE, SEBI) simply requires adding an implementation of `BaseConnector` to the connector registry.
2. **Zero Bottlenecks:** The Orchestrator never slows down waiting for external target websites to respond. High latency external crawls are isolated to the connector tier.
3. **Infinite Horizontal Scaling:** Run 1 or 50 connector worker instances across machines. They automatically share the Redis consumer group with load balanced work delivery.
4. **Fault Tolerance:** If a connector worker crashes mid-fetch, Redis maintains the message in the pending entries list (PEL) to be retried.
5. **Stateless Connector:** The connector never touches MongoDB. It reads from Redis and writes to Redis, keeping database credentials and persistence policies centralized in the Orchestrator.

---

## 2. The Request State Lifecycle

Every crawl task in MongoDB's `rss_requests` collection travels through an auditable, strict state machine:

```text
              ┌──────────────┐
              │   pending    │  <-- Job seeded in MongoDB
              └──────┬───────┘
                     │
                     │ (Atomically claimed by request_service.py)
                     ▼
              ┌──────────────┐
              │  processing  │  <-- Dispatched to Redis Stream with 'dispatched_at'
              └──────┬───────┘
                     │
        ┌────────────┴────────────┐
        │ (Connector succeeds)    │ (Connector errors / invalid feed)
        ▼                         ▼
 ┌──────────────┐          ┌──────────────┐
 │  completed   │          │    failed    │
 └──────────────┘          └──────────────┘
```

1. **`pending`**: Job exists in MongoDB with target metadata and `status: "pending"`, awaiting worker pickup.
2. **`processing`**: Atomically claimed by `request_service.py`, `claimed_at` and `dispatched_at` timestamps recorded, task pushed to Redis Stream with `stream_message_id` stored for traceability.
3. **`completed`**: Connector fetched the feed, parsed articles, and Orchestrator's `result_service.py` bulk-upserted them into `rss_items` while updating request status to `completed`.
4. **`failed`**: URL was unreachable, timed out, or had malformed data. `error_message` is recorded for debugging.

---

## 3. Repository Directory Structures

### `argus-orchestrator/`
```text
argus-orchestrator/
├── .env                      # Local environment overrides (passwords, URLs)
├── .env.example              # Template environment variables
├── requirements.txt          # Python dependencies (fastapi, motor, redis, pydantic-settings, uvicorn)
├── PROJECT_GUIDE.md          # THIS FILE: Full architecture & code walkthrough
├── app/
│   ├── main.py               # FastAPI entrypoint with lifespan managing background workers
│   ├── api/
│   │   └── health.py         # /health and /ready probe endpoints
│   ├── core/
│   │   ├── config.py         # Pydantic Settings loader
│   │   └── logging.py        # Centralized logger setup
│   ├── platform/
│   │   ├── mongodb.py        # Async Motor client, news_queries, rss_requests & rss_items collections
│   │   └── redis.py          # Async Redis client with decode_responses=True
│   ├── schemas/
│   │   ├── query.py          # NewsQuery model for news_queries collection
│   │   ├── request.py        # Generic ConnectorRequest outbound schema
│   │   └── result.py         # Inbound schema (RSSItem and CrawlResult)
│   ├── services/
│   │   ├── bing_service.py   # Bing search RSS URL builder
│   │   ├── request_service.py # Atomic claim, metadata serialization, and Redis dispatch
│   │   └── result_service.py  # Article deduplication bulk upsert & job completion
│   ├── test/
│   │   └── test_bing.py      # Standalone verification test for Bing RSS search
│   └── workers/
│       ├── request_worker.py  # Continuous MongoDB polling dispatcher daemon
│       └── result_worker.py   # Continuous Redis results consumer daemon
└── scripts/
    ├── add_rss_request.py    # CLI tool to seed pending multi-source requests into MongoDB
    └── seed_news_query.py    # Seed query dictionary for targeted Bing news searches
```

### `argus-connector/`
```text
argus-connector/
├── .env                      # Redis URL, stream names, consumer group configuration
├── .env.example              # Template environment variables
├── requirements.txt          # Python dependencies (redis, httpx, feedparser, pydantic-settings, fastapi, uvicorn)
├── app/
│   ├── main.py               # FastAPI entrypoint with lifespan managing ConnectorWorker
│   ├── core/
│   │   ├── config.py         # Pydantic Settings (Redis URLs, stream names)
│   │   └── logging.py        # Centralized logger setup
│   ├── platform/
│   │   ├── mongodb.py        # Empty stub (stateless connector does not use Mongo)
│   │   └── redis.py          # Async Redis client with decode_responses=True
│   ├── schemas/
│   │   └── connector.py      # Generic ConnectorRequest inbound schema
│   ├── connectors/
│   │   ├── base.py           # BaseConnector abstract base class (ABC)
│   │   ├── registry.py       # Central dynamic connector registry & lookup
│   │   └── rss/
│   │       ├── connector.py  # RSSConnector implementing BaseConnector
│   │       ├── model.py      # RSSItem & CrawlResult Pydantic models with SHA-256 fingerprinting
│   │       └── parser.py     # Async HTTP fetcher + feedparser article extractor
│   ├── test/
│   │   ├── test_bing_connector.py # Live test for RSSConnector with Bing query
│   │   └── test_bing_parser.py    # Live test for fetch_and_parse_rss with Bing
│   └── workers/
│       └── connector_worker.py # Generic continuous Redis Stream consumer loop & dispatcher
```

---

## 4. argus-orchestrator: File-by-File Walkthrough

---

### File 1: `app/core/config.py`
**Purpose:** Single source of truth for all configuration values. Reads from `.env` and provides strongly-typed settings.

```python
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_name: str = "argus-orchestrator"
    app_env: str = "development"
    mongo_url: str = "mongodb://localhost:27017"
    mongo_database: str = "argus"
    redis_url: str = "redis://localhost:6379/0"
    connector_stream: str = "queue:connector:rss"     # Stream we PUSH jobs to
    result_stream: str = "connector:rss:results"      # Stream we READ results from
    consumer_group: str = "orchestrator-results"       # Consumer group for result reading
    consumer_name: str = "orchestrator-1"              # Unique worker name

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore"   # Prevents crashes if unknown env vars exist
    )

settings = Settings()
```

---

### File 2: `app/core/logging.py`
**Purpose:** Standardizes log output across all workers with consistent timestamp, level, module name formatting.

```python
import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s |"
    )
```

---

### File 3: `app/platform/mongodb.py`
**Purpose:** Sets up the async MongoDB client using `motor` and defines collection handles.

**Key Concept:** `motor` is async — database queries do not freeze the Python process; other tasks continue running concurrently via `asyncio`.

```python
from motor.motor_asyncio import AsyncIOMotorClient
from app.core.config import settings

mongo_client = AsyncIOMotorClient(settings.mongo_url)
database = mongo_client[settings.mongo_database]

news_queries = database["news_queries"] # Target queries dictionary for news search
rss_requests = database["rss_requests"]   # Job queue & status tracking
rss_items = database["rss_items"]         # Parsed and stored articles

async def check_mongodb():
    """Pings MongoDB admin to verify connection is alive."""
    try:
        await mongo_client.admin.command("ping")
        return True
    except Exception:
        return False
```

---

### File 4: `app/platform/redis.py`
**Purpose:** Sets up the async Redis connection pool.

**Key Setting:** `decode_responses=True` automatically converts Redis byte strings to UTF-8 Python strings, avoiding `b'...'` prefixes everywhere.

```python
import redis.asyncio as redis
from app.core.config import settings

redis_client = redis.from_url(settings.redis_url, decode_responses=True)

async def check_redis():
    """Pings Redis to confirm broker reachability."""
    try:
        response = await redis_client.ping()
        return response is True
    except Exception:
        return False
```

---

### File 5: `app/main.py`
**Purpose:** FastAPI application entrypoint. Uses `lifespan` context manager to automatically start both background workers when Uvicorn boots, and cleanly stop them on shutdown.

```python
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.health import router as health_router
from app.core.logging import setup_logging
from app.workers.request_worker import RequestWorker
from app.workers.result_worker import ResultWorker

setup_logging()

request_worker = RequestWorker(poll_interval=2.0)
result_worker = ResultWorker(block_ms=2000, batch_size=10)
background_tasks = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Argus Orchestrator starting up on Port 8000...")
    t1 = asyncio.create_task(request_worker.start())  # Outbound dispatcher
    t2 = asyncio.create_task(result_worker.start())   # Inbound ingestor
    background_tasks.extend([t1, t2])
    yield                                              # Server runs here
    print("🛑 Argus Orchestrator shutting down...")
    request_worker.stop()
    result_worker.stop()
    await asyncio.gather(*background_tasks, return_exceptions=True)

app = FastAPI(title="Argus Orchestrator", version="2.0", lifespan=lifespan)
app.include_router(health_router)

@app.get("/")
async def root():
    return {"service": "argus-orchestrator", "status": "running"}
```

**Run with:**
```bash
uvicorn app.main:app --port 8000 --reload
```

---

### File 6: `app/api/health.py`
**Purpose:** Liveness and readiness probe endpoints for monitoring/infrastructure.

- `GET /health` — Liveness probe. Returns 200 if the process is running.
- `GET /ready` — Readiness probe. Checks both MongoDB and Redis. Returns 503 if either database is down.

---

### File 7: `scripts/add_rss_request.py`
**Purpose:** Developer CLI tool to seed a `pending` multi-source crawl request into MongoDB.

**Updated Generic Schema:**
```python
import uuid
from datetime import datetime, timezone
from app.platform.mongodb import rss_requests

async def seed_request():
    request_doc = {
        "request_id": f"req_{uuid.uuid4().hex[:12]}",
        "source": "bing",
        "source_type": "rss",
        "url": "https://www.bing.com/search?q=business+news&format=rss",
        "metadata": {
            "query": "business news",
            "topic": "business"
        },
        "status": "pending",
        "priority": "HIGH",
        "created_at": datetime.now(timezone.utc),
        "claimed_at": None,
        "dispatched_at": None,
        "completed_at": None,
        "updated_at": None,
        "stream_message_id": None,
        "items_count": 0,
        "new_items_count": 0,
        "error_message": None
    }
    result = await rss_requests.insert_one(request_doc)
    print(f"Seeded pending request with _id: {result.inserted_id}")
```

```bash
# Run from argus-orchestrator:
python -m scripts.add_rss_request
```

---

### File 8: `app/schemas/request.py`
**Purpose:** Pydantic model enforcing the outbound data contract dispatched to Redis Stream `queue:connector:rss`.

```python
from typing import Any
from pydantic import BaseModel, Field

class ConnectorRequest(BaseModel):
    request_id: str
    source: str
    source_type: str
    source_url: str
    metadata: dict[str, Any] = Field(default_factory=dict)
```

**Why Generic?**
Instead of restricting requests to RSS feeds, `ConnectorRequest` represents any source (e.g. `source="nse"`, `source_type="announcements"`, `source_url="..."`, `metadata={"ticker": "RELIANCE"}`).

---

### File 9: `app/services/request_service.py`
**Purpose:** Core outbound business logic — atomically claiming a pending task from MongoDB, validating the schema, stringifying metadata for Redis, and updating audit trails.

**Key Mechanics:**
1. **Atomic Claiming:**
   ```python
   claimed_doc = await rss_requests.find_one_and_update(
       {"status": "pending"},
       {"$set": {"status": "processing", "claimed_at": datetime.now(timezone.utc)}},
       return_document=ReturnDocument.AFTER
   )
   ```
2. **Schema Construction & Validation:**
   ```python
   request_obj = ConnectorRequest(
       request_id=request_id,
       source=claimed_doc["source"],
       source_type=claimed_doc["source_type"],
       source_url=claimed_doc["url"],
       metadata=claimed_doc.get("metadata", {}),
   )
   ```
3. **Redis Stream Serialization:**
   Redis streams store flat key-value pairs where values must be strings or numbers. Dicts cannot be passed directly. The service JSON-encodes `metadata`:
   ```python
   dispatch_time = datetime.now(timezone.utc)
   redis_payload = request_obj.model_dump()
   redis_payload["metadata"] = json.dumps(redis_payload["metadata"])

   stream_message_id = await redis_client.xadd(
       settings.connector_stream,
       redis_payload
   )
   ```
4. **Audit Back-Link:**
   Updates MongoDB with both `stream_message_id` and `dispatched_at` timestamp.
5. **Two-Tier Error Handling:**
   - `ValidationError`: Marks request as `"failed"`.
   - Network `Exception`: Rolls status back to `"pending"` for automatic retry when Redis recovers.

---

### File 10: `app/workers/request_worker.py`
**Purpose:** Autonomous background dispatcher daemon that calls `dispatch_request()` in a loop.

**Key Mechanics:**
- **Drain Mode:** When a task is dispatched, sleeps only `10ms` and immediately loops to clear the backlog.
- **Idle Mode:** When no tasks are pending, waits `poll_interval` (2 seconds) using `asyncio.wait_for(self._stop_event.wait(), timeout=...)`.
- **Clean Shutdown:** Catches cancellation and stop signals without dropping tasks.

---

### File 11: `app/schemas/result.py`
**Purpose:** Inbound Pydantic data models defining the payload received from the connector via Redis Stream `connector:rss:results`.

- **`RSSItem`**: A single parsed article stored in `rss_items`:
  - `item_hash`: SHA-256 fingerprint of the normalized URL (`link.strip().lower()`).
  - `request_id`: Originating job ID.
  - `source`: Publisher/source identifier (e.g. `"bing"`, `"indian_express"`).
  - `source_type`: Connector category (e.g. `"rss"`).
  - `title`: Article title.
  - `link`: Canonical URL.
  - `description`: Optional raw description snippet.
  - `summary`: Cleaned summary text.
  - `author`: Article author.
  - `published_at`: Timezone-aware UTC publication timestamp.
  - `created_at`: Ingestion timestamp.
  - `model_post_init()`: Automatically computes `item_hash` from `link` if not already set.
- **`CrawlResult`**:
  - `request_id`: Correlation ID of the original request.
  - `status`: `"success"` or `"failed"`.
  - `item_count`: Total number of items in the batch (standardized from `items_count`).
  - `items`: List of `RSSItem` objects.
  - `error_message`: Error details if status is `"failed"`.
  - `fetched_at`: UTC timestamp of fetch completion.

---

### File 12: `app/services/result_service.py`
**Purpose:** Inbound business logic — saving articles to MongoDB with zero duplicates and updating request status.

**Key Mechanics:**
1. **Index Assurance (`ensure_indexes`)**:
   - `rss_items.item_hash`: Unique index for deduplication.
   - `rss_items.request_id`: Index for looking up items by task.
   - `rss_requests.request_id`: Unique index for status updates.
2. **Zero-Duplicate Bulk Upsert (`$setOnInsert`)**:
   ```python
   UpdateOne(
       {"item_hash": item.item_hash},
       {"$setOnInsert": item.model_dump()},
       upsert=True
   )
   ```
   If the hash exists, MongoDB skips it without error. If new, it inserts the document. `$setOnInsert` ensures existing articles preserve their original `created_at` timestamp.
3. **Status Transition:**
   - On `"success"`: Updates `rss_requests` to `"completed"` with `items_count`, `new_items_count`, and `completed_at`.
   - On `"failed"`: Updates `rss_requests` to `"failed"` with `error_message`.

---

### File 13: `app/workers/result_worker.py`
**Purpose:** Autonomous background consumer daemon that reads crawl results from Redis Stream `connector:rss:results` and passes them to `process_crawl_result()`.

**Key Mechanics:**
1. Ensures consumer group exists (`xgroup_create` with `mkstream=True`).
2. Reads messages using `xreadgroup` (`">"` for new undelivered messages).
3. Parses JSON payloads (handles both `"payload"` and `"items"` string encodings).
4. Only executes `xack()` **after** database persistence succeeds (guaranteeing at-least-once delivery).

---

### File 14: `app/schemas/query.py`
**Purpose:** Pydantic model for persistent topic and keyword search queries stored in MongoDB's `news_queries` collection.

```python
from datetime import datetime, timezone
from typing import Any
from pydantic import BaseModel, Field

class NewQuery(BaseModel):
    source: str = "bing"
    source_type: str = "rss"
    topic: str
    query: str
    enabled: bool = True
    priority: str = "HIGH"
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
```

---

### File 15: `app/services/bing_service.py`
**Purpose:** URL construction utility for Bing News RSS search queries. Encodes search phrases into valid RSS endpoints.

```python
from urllib.parse import quote_plus

def build_bing_rss_url(query: str) -> str:
    encoded_query = quote_plus(query.strip())
    return f"https://www.bing.com/search?q={encoded_query}&format=rss"
```

---

### File 16: `scripts/seed_news_query.py`
**Purpose:** Database initialization script that seeds target news keywords into `news_queries` collection.

**Key Mechanics:**
- Creates a unique compound index on `("source", 1), ("query", 1)` to prevent duplicate search jobs.
- Seeds default topics: `"business news"`, `"corporate news"`, `"company news"`, and `"corporate sector"`.
- Uses `$setOnInsert` via `upsert=True` to maintain idempotency without overwriting existing query statuses.

```bash
# Run from argus-orchestrator:
python scripts/seed_news_query.py
```

---

### File 17: `app/test/test_bing.py`
**Purpose:** Standalone verification script for Bing Search RSS feeds using `requests` and `feedparser`.

- Generates query URL using `build_bing_rss_url("business news")`.
- Fetches feed using realistic browser `User-Agent`.
- Parses returned XML and prints article titles, links, and publication timestamps to verify feed availability.

---

## 5. argus-connector: File-by-File Walkthrough

The connector is a **stateless, database-agnostic scraping service**. It communicates solely through Redis Streams — it never connects to MongoDB.

---

### Connector File 1: `app/core/config.py`
**Purpose:** Reads connector-specific settings from `.env`.

```python
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_name: str = "argus-connector"
    redis_url: str = "redis://localhost:6379/0"
    connector_stream: str = "queue:connector:rss"   # Stream we READ jobs from
    result_stream: str = "connector:rss:results"    # Stream we WRITE results to
    consumer_group: str = "rss_connectors"           # Consumer group (shared by workers)
    consumer_name: str = "connector_worker-1"        # Unique ID for THIS worker instance

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore"
    )

settings = Settings()
```

---

### Connector File 2: `app/core/logging.py`
**Purpose:** Identical logging setup to Orchestrator for consistent log output across both microservices.

---

### Connector File 3: `app/platform/redis.py`
**Purpose:** Async Redis client initialized with `decode_responses=True`.

---

### Connector File 4: `app/schemas/connector.py`
**Purpose:** Inbound request contract representing tasks pulled from the Redis stream.

```python
from typing import Any, Optional
from pydantic import BaseModel, Field

class ConnectorRequest(BaseModel):
    request_id: str
    source: str
    source_type: Optional[str] = None
    source_url: Optional[str] = None    
    metadata: dict[str, Any] = Field(default_factory=dict)
```

---

### Connector File 5: `app/connectors/base.py`
**Purpose:** Abstract Base Class (ABC) defining the standard interface that every data connector must implement.

```python
from abc import ABC, abstractmethod

class BaseConnector(ABC):

    @abstractmethod
    async def fetch(self, request):
        """Fetch and return structured crawl results for the given ConnectorRequest."""
        pass
```

**Why This Abstraction?**
Every future connector (NSE, BSE, SEBI, Bing, Twitter/X) implements this exact `fetch()` method. The worker daemon interacts only with `BaseConnector`, making the worker 100% connector-agnostic.

---

### Connector File 6: `app/connectors/registry.py`
**Purpose:** Central connector factory and registry mapping source types to connector instances.

```python
from app.connectors.rss.connector import RSSConnector

CONNECTORS = {
    "rss": RSSConnector(),
}

def get_connector(source_type: str):
    connector = CONNECTORS.get(source_type)
    if not connector:
        raise ValueError(f"Unsupported connector: {source_type}")
    return connector
```

---

### Connector File 7: `app/connectors/rss/connector.py`
**Purpose:** Concrete implementation of `BaseConnector` for RSS feeds.

```python
from app.connectors.base import BaseConnector
from app.connectors.rss.parser import fetch_and_parse_rss

class RSSConnector(BaseConnector):
    
    async def fetch(self, request):
        return await fetch_and_parse_rss(
            request_id=request.request_id,
            source=request.source,
            source_type=request.source_type,
            url=request.source_url,
        )
```

---

### Connector File 8: `app/connectors/rss/model.py`
**Purpose:** Data models for RSS articles and batch results. Standardized to match the Orchestrator's `app/schemas/result.py`.

```python
import hashlib
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel, Field

class RSSItem(BaseModel):
    item_hash: Optional[str] = None
    request_id: str
    source: str
    source_type: str
    title: str
    link: str                    # Standardized 'link' matches Orchestrator
    description: Optional[str] = None
    summary: Optional[str] = None
    author: Optional[str] = None
    published_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def compute_hash(self) -> str:
        """Generates SHA-256 fingerprint from normalized link."""
        raw = self.link.strip().lower()
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def model_post_init(self, __context):
        if not self.item_hash and self.link:
            self.item_hash = self.compute_hash()

class CrawlResult(BaseModel):
    request_id: str
    status: str = "success"
    item_count: int = 0          # Standardized from items_count
    items: List[RSSItem] = Field(default_factory=list)
    error_message: Optional[str] = None
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
```

---

### Connector File 9: `app/connectors/rss/parser.py`
**Purpose:** Asynchronous HTTP fetcher and parser using `httpx.AsyncClient` and `feedparser`.

- Signature: `fetch_and_parse_rss(request_id: str, source: str, source_type: str, url: str) -> CrawlResult`.
- Propagates `source` and `source_type` down to each created `RSSItem` for end-to-end data provenance.
- Uses a realistic `User-Agent` header to prevent bot blocking.
- Converts publication timestamps (`published_parsed` / `updated_parsed`) to timezone-aware UTC datetimes.
- Returns `CrawlResult(status="failed", error_message=...)` on network timeouts, HTTP errors, or unparseable feeds.

---

### Connector File 10: `app/workers/connector_worker.py`
**Purpose:** The generic continuous worker daemon that pulls tasks from Redis, safely parses parameters and metadata, resolves the connector via the registry, executes `fetch()`, publishes results, and acknowledges with `xack`.

**Key Mechanics:**
1. **Safe Metadata Deserialization:**
   ```python
   metadata_raw = fields.get("metadata", {})
   try:
       metadata = json.loads(metadata_raw)
   except (TypeError, json.JSONDecodeError):
       logger.warning(f"Invalid metadata for message {message_id}. Using empty metadata.")
       metadata = {}
   ```
2. **Validation Guard:**
   Ensures `request_id`, `source`, `source_type`, and `source_url` exist. If missing, logs error and acknowledges message with `xack` to prevent poisoned message loops.
3. **Dynamic Connector Dispatch:**
   ```python
   request = ConnectorRequest(
       request_id=request_id,
       source=source,
       source_type=source_type,
       source_url=source_url,
       metadata=metadata,
   )
   connector = get_connector(request.source_type)
   crawl_result = await connector.fetch(request)
   ```
4. **Publish & Acknowledge:**
   Publishes `crawl_result.model_dump_json()` to `result_stream` and calls `redis_client.xack()`.

---

### Connector File 11: `app/main.py`
**Purpose:** FastAPI service entrypoint managing `ConnectorWorker` lifecycle via `lifespan`.

```python
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.core.logging import setup_logging
from app.platform.redis import check_redis
from app.workers.connector_worker import ConnectorWorker

setup_logging()

worker = ConnectorWorker(block_ms=2000, batch_size=5)
worker_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_task
    print("Argus Connector starting up on port 8001...")
    worker_task = asyncio.create_task(worker.start())
    yield
    print("Argus Connector shutting down...")
    worker.stop()
    if worker_task:
        await asyncio.gather(worker_task, return_exceptions=True)
    print("Argus Connector shutdown gracefully!")

app = FastAPI(title="Argus Connector", version="2.0", lifespan=lifespan)

@app.get("/")
async def root():
    return {"service": "argus-connector", "status": "running"}

@app.get("/health")
async def health():
    redis_ok = await check_redis()
    return {"status": "Healthy" if redis_ok else "unhealthy", "redis": "connected" if redis_ok else "disconnected"}
```

**Run with:**
```bash
uvicorn app.main:app --port 8001 --reload
```

---

### Connector File 12: `app/test/` (Verification Test Suite)
**Purpose:** Standalone verification scripts to validate XML parser parsing and dynamic connector registry execution without spinning up full service daemons.

- **`test_bing_parser.py`**:
  Directly invokes `fetch_and_parse_rss()` against Bing News Search query RSS feeds. Verifies that HTTP client options (redirects, user-agent) successfully retrieve live search results and map them into valid `RSSItem` objects.
- **`test_bing_connector.py`**:
  Instantiates a `ConnectorRequest(source="bing", source_type="rss", ...)` and queries `get_connector("rss")`. Validates that dynamic connector dispatch properly resolves `RSSConnector`, calls `fetch()`, and returns standard `CrawlResult` models.

```bash
# Run tests from argus-connector:
python app/test/test_bing_parser.py
python app/test/test_bing_connector.py
```

---

## 6. End-to-End Pipeline: How to Run the Full System

### Step 1: Start Orchestrator (Port 8000)
```bash
# In argus-orchestrator directory:
.\venv\Scripts\activate
uvicorn app.main:app --port 8000 --reload
```

### Step 2: Start Connector (Port 8001)
```bash
# In argus-connector directory:
.\venv\Scripts\activate
uvicorn app.main:app --port 8001 --reload
```

### Step 3: Seed a Test Job
```bash
# In argus-orchestrator directory:
.\venv\Scripts\activate
python -m scripts.add_rss_request
```

### Execution Flow:
1. `add_rss_request.py` inserts a `{source: "indian_express", source_type: "rss", url: "...", status: "pending"}` document into MongoDB `rss_requests`.
2. Orchestrator's `request_worker` claims it atomically, sets `status: "processing"`, serializes `metadata` to JSON, and publishes `ConnectorRequest` to Redis `queue:connector:rss`.
3. Connector's `connector_worker` pulls the task via `xreadgroup`.
4. `get_connector("rss")` resolves `RSSConnector`.
5. `RSSConnector.fetch()` downloads XML via `httpx` and parses articles via `feedparser`.
6. Connector publishes `CrawlResult` JSON to Redis `connector:rss:results` and sends `xack`.
7. Orchestrator's `result_worker` pulls the result, runs `save_rss_items()`, and bulk-upserts articles into MongoDB `rss_items` using `item_hash` deduplication.
8. Orchestrator marks request as `status: "completed"` in `rss_requests` with article counts and completion timestamp.

---

## 7. Bugs Encountered & Fixes Applied

### Bug 1: `ModuleNotFoundError: No module named 'motor'`
- **Where:** Orchestrator startup.
- **Fix:** Added `motor>=3.7.0` to `requirements.txt`.

### Bug 2: Pydantic `extra_forbidden` crash on startup
- **Where:** `app/core/config.py` — `.env` had extra environment variables.
- **Fix:** Added `extra="ignore"` to `SettingsConfigDict`.

### Bug 3: `commad` typo in MongoDB ping
- **Where:** `app/platform/mongodb.py`.
- **Fix:** Fixed `mongo_client.admin.commad("ping")` to `command("ping")`.

### Bug 4: `decode_response = True` misspelling
- **Where:** `app/platform/redis.py`.
- **Fix:** Changed parameter to `decode_responses=True`.

### Bug 5: `url_has_1` duplicate index collision in MongoDB
- **Symptom:** `DuplicateKeyError` when inserting tasks.
- **Fix:** Dropped the old non-sparse index `url_has_1` via MongoDB.

### Bug 6: `AttributeError: 'RSSItems' object has no attribute 'link'`
- **Where:** `argus-connector/app/connectors/rss/model.py`.
- **Fix:** Renamed attribute from `url` to `link` throughout model and parser.

### Bug 7: Connector serialized `url` but Orchestrator expected `link`
- **Symptom:** Pydantic validation error in Orchestrator `RSSItem`.
- **Fix:** Standardized on `link` across both services.

### Bug 8: `result_stream` name mismatch
- **Symptom:** Connector published to `connector:rss:result` (singular) while Orchestrator listened on `connector:rss:results` (plural).
- **Fix:** Standardized to `connector:rss:results`.

### Bug 9: RSS parser items loop indented inside bozo error block
- **Symptom:** Parser returned 0 items for valid feeds.
- **Fix:** Moved `items = []` and entry loop out of the early return block.

### Bug 10: `run_worker()` indented inside worker class
- **Symptom:** `ImportError` on module import.
- **Fix:** Corrected indentation to module-level function.

### Bug 11: `xgroup_create(group=...)` keyword argument
- **Symptom:** `NOGROUP` error in Redis consumer group creation.
- **Fix:** Changed parameter to `groupname=...`.

### Bug 12: Worker entry loop indented under `if not entries: continue`
- **Symptom:** Messages were received from Redis but never processed.
- **Fix:** Adjusted loop indentation outside the guard block.

### Bug 13: Redis Stream field serialization for dictionaries
- **Symptom:** Attempting to pass `metadata: dict` directly to `redis_client.xadd()` fails because Redis stream fields must be strings, bytes, or integers.
- **Fix:** In Orchestrator's `request_service.py`, serialized metadata with `json.dumps()`. In Connector's `connector_worker.py`, parsed `metadata_raw` with `json.loads()` and provided empty dict fallback for invalid payloads.

### Bug 14: Worker rename import mismatch in `argus-connector/app/main.py`
- **Symptom:** Renaming `rss_worker.py` to `connector_worker.py` caused `ModuleNotFoundError: No module named 'app.workers.rss_worker'` when starting the connector via Uvicorn.
- **Fix:** Updated `app/main.py` to import `ConnectorWorker` from `app.workers.connector_worker`.

### Bug 15: Cross-Service Model Name & Batch Count Discrepancies
- **Symptom:** Connector schema used `RSSItems` (plural) and `items_count`, while Orchestrator expected `RSSItem` (singular) and `item_count`. Articles also lacked explicit `source`, `source_type`, and `description` tracking.
- **Fix:** Aligned both microservices to `RSSItem` and `item_count`, and added `source`, `source_type`, and `description` to the Pydantic models in both repositories.

### Bug 16: Missing Data Provenance in RSS Parser
- **Symptom:** When `RSSConnector.fetch()` called `fetch_and_parse_rss()`, only `request_id` and `url` were passed. As a result, extracted articles had no record of which provider (`source`) or crawler method (`source_type`) discovered them.
- **Fix:** Updated `fetch_and_parse_rss(request_id, source, source_type, url)` to accept source attributes and attach them to every created `RSSItem`.

### Bug 17: Unwanted `_typeshed` Import in `app/schemas/query.py`
- **Symptom:** IDE auto-imported `from _typeshed import structseq`, crashing with `ModuleNotFoundError: No module named '_typeshed'` when importing `NewQuery`.
- **Fix:** Removed the unused `_typeshed` import from `app/schemas/query.py`.

---

## 8. Current Roadmap & Project Progress

```text
[x] Phase 1:  Environment & Async Database Drivers (Motor & Redis)
[x] Phase 2:  Configuration & Logging Setup
[x] Phase 3:  Outbound Request Schema & Atomic Dispatch Service
[x] Phase 4:  Outbound Request Worker Daemon (app/workers/request_worker.py)
[x] Phase 5:  Inbound Result Schema & Deduplication Service (app/services/result_service.py)
[x] Phase 6:  Inbound Result Worker Consumer (app/workers/result_worker.py)
[x] Phase 7:  FastAPI Lifespan Integration (Uvicorn servers on Ports 8000 & 8001)
[x] Phase 8:  End-to-End RSS Pipeline Verification (MongoDB <-> Redis <-> Connector)
[x] Phase 9:  Generic Multi-Connector Architecture & Registry Pattern
              - app/connectors/base.py (BaseConnector ABC)
              - app/connectors/registry.py (Dynamic connector registry & get_connector)
              - app/connectors/rss/connector.py (RSSConnector implementation)
              - app/schemas/connector.py (Generic ConnectorRequest schema)
              - app/workers/connector_worker.py (Generic ConnectorWorker daemon)
              - app/services/request_service.py (dispatch_request + JSON metadata)
[x] Phase 15: Bing News RSS Engine & Custom Query Generation
              - app/services/bing_service.py (build_bing_rss_url query generator)
              - app/schemas/query.py (NewQuery schema for news search tracking)
              - scripts/seed_news_query.py (News query dictionary seeder with unique index)
              - app/test/test_bing.py (Direct HTTP + feedparser verification)
              - argus-connector/app/test/ (Live test_bing_parser & test_bing_connector suites)
──────────────────────────────────────────────────────────────────────────────────────────
[ ] Phase 10: NSE Announcements Connector (argus-connector/app/connectors/nse/)
[ ] Phase 11: BSE Corporate Announcements Connector
[ ] Phase 12: SEBI Orders & Circulars Connector (HTML scraper + PDF link extractor)
[ ] Phase 13: SAT (Securities Appellate Tribunal) Orders Connector
[ ] Phase 14: Stocktwits Social Feed Connector (REST API + JSON)
[ ] Phase 16: REST API Endpoint (POST /api/feeds) to manage requests via HTTP
[ ] Phase 17: Scheduler Integration (APScheduler for automatic periodic re-crawls)
[ ] Phase 18: Unified Data Model (single 'market_intelligence_items' collection)
```
