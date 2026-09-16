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
5. [argus-connector: File-by-File Walkthrough](#5-argus-connector-file-by-file-walkthrough)
   - [Config: `app/core/config.py`](#connector-file-1-appcoreconfigpy)
   - [Logging: `app/core/logging.py`](#connector-file-2-appcoreloggingpy)
   - [Redis Client: `app/platform/redis.py`](#connector-file-3-appplatformredispy)
   - [Data Model: `app/connectors/rss/model.py`](#connector-file-4-appconnectorsrssmodelpy)
   - [RSS Parser: `app/connectors/rss/parser.py`](#connector-file-5-appconnectorsrssparserpy)
   - [RSS Worker: `app/workers/rss_worker.py`](#connector-file-6-appworkersrss_workerpy)
   - [FastAPI Entrypoint: `app/main.py`](#connector-file-7-appmainpy)
6. [End-to-End Pipeline: How to Run the Full System](#6-end-to-end-pipeline-how-to-run-the-full-system)
7. [Bugs Encountered & Fixes Applied](#7-bugs-encountered--fixes-applied)
8. [Current Roadmap & Project Progress](#8-current-roadmap--project-progress)

---

## 1. System Architecture: What Are We Building?

Argus 2.0 is an **event-driven, decoupled web intelligence and content ingestion system**.

Instead of building a monolithic script that downloads feeds, parses articles, and saves them in one place, the system is split into two independent microservices that communicate exclusively through **Redis Streams**:

```
┌────────────────────────────────────────────────────────────────────────┐
│                    ARGUS ORCHESTRATOR  (Port 8000)                     │
│                                                                        │
│  scripts/add_rss_request.py                                            │
│         │ (1. Insert 'pending' task into MongoDB)                      │
│         ▼                                                              │
│  [MongoDB: rss_requests]                                               │
│         │ (2. Atomic Claim: status -> 'processing')                    │
│         ▼                                                              │
│  [request_worker.py] --> [request_service.py]                          │
│                                  │                                     │
│                                  │ (3. xadd: Outbound Task)            │
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
│  [rss_worker.py] --> [parser.py]                                       │
│                           │                                            │
│                           │ (5. httpx fetches XML from internet)       │
│                           │ (6. feedparser extracts articles)          │
│                           ▼                                            │
│                    [CrawlResult payload]                               │
│                           │                                            │
│                           │ (7. xadd: Publish result JSON)             │
└───────────────────────────┼────────────────────────────────────────────┘
                            │
                            ▼
          ┌──────────────────────────────────────────────────┐
          │  Redis Stream: 'connector:rss:results'           │
          └──────────────────────────────────────────────────┘
                            │
                            ▼ (8. xreadgroup via consumer group)
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
1. **Zero Bottlenecks:** The Orchestrator never slows down waiting for websites to respond. Slow feeds block only the connector.
2. **Infinite Scaling:** Run 1 or 50 connector instances across machines. They share the Redis consumer group automatically — zero code changes needed.
3. **Fault Tolerance:** If a connector crashes mid-fetch, Redis keeps the message in the pending list. It will be retried automatically.
4. **No Database Access in Connector:** The connector is purely stateless. It speaks Redis only. All MongoDB persistence is handled by the Orchestrator.

---

## 2. The Request State Lifecycle

Every feed crawl task in MongoDB's `rss_requests` collection travels through a strict, auditable state machine:

```
              ┌──────────────┐
              │   pending    │  <-- Job seeded in MongoDB
              └──────┬───────┘
                     │
                     │ (Atomically claimed by request_service.py)
                     ▼
              ┌──────────────┐
              │  processing  │  <-- Dispatched to Redis Stream
              └──────┬───────┘
                     │
        ┌────────────┴────────────┐
        │ (Connector succeeds)    │ (Connector errors / invalid URL)
        ▼                         ▼
 ┌──────────────┐          ┌──────────────┐
 │  completed   │          │    failed    │
 └──────────────┘          └──────────────┘
```

1. **`pending`**: Job exists in MongoDB but has not been picked up yet.
2. **`processing`**: Atomically claimed, `claimed_at` recorded, task pushed to Redis with `stream_message_id` stored.
3. **`completed`**: Connector scraped the feed, returned articles, Orchestrator saved them to `rss_items`.
4. **`failed`**: URL was unreachable, timed out, or returned invalid RSS. `error_message` is recorded.

---

## 3. Repository Directory Structures

### `argus-orchestrator/`
```text
argus-orchestrator/
├── .env                      # Local environment overrides (passwords, URLs)
├── .env.example              # Template environment variables
├── requirements.txt          # Python dependencies (fastapi, motor, redis, pydantic-settings, uvicorn)
├── PROJECT_GUIDE.md          # THIS FILE: Full architecture & code guide
├── app/
│   ├── main.py               # FastAPI entrypoint with lifespan managing both workers
│   ├── api/
│   │   └── health.py         # /health and /ready probe endpoints
│   ├── core/
│   │   ├── config.py         # Pydantic Settings loader
│   │   └── logging.py        # Centralized logger setup
│   ├── platform/
│   │   ├── mongodb.py        # Async Motor client, rss_requests & rss_items collections
│   │   └── redis.py          # Async Redis client
│   ├── schemas/
│   │   ├── request.py        # Outbound schema (sent to Redis queue)
│   │   └── result.py         # Inbound schema (received from Redis results)
│   ├── services/
│   │   ├── request_service.py # Atomic claim & Redis xadd dispatch
│   │   └── result_service.py  # Article upsert & request completion
│   └── workers/
│       ├── request_worker.py  # Continuous MongoDB polling daemon
│       └── result_worker.py   # Continuous Redis results consumer daemon
└── scripts/
    └── add_rss_request.py    # CLI tool to seed pending crawl requests into MongoDB
```

### `argus-connector/`
```text
argus-connector/
├── .env                      # Redis URL, stream names, consumer group name
├── .env.example              # Template environment variables
├── requirements.txt          # Python dependencies (redis, httpx, feedparser, pydantic-settings, fastapi, uvicorn)
├── app/
│   ├── main.py               # FastAPI entrypoint with lifespan managing RSS worker
│   ├── core/
│   │   ├── config.py         # Pydantic Settings (Redis URLs, stream names)
│   │   └── logging.py        # Centralized logger setup (same format as Orchestrator)
│   ├── platform/
│   │   └── redis.py          # Async Redis client with decode_responses=True
│   ├── connectors/
│   │   └── rss/
│   │       ├── model.py      # Pydantic data models (RSSItems & CrawlResult)
│   │       └── parser.py     # Async HTTP fetcher + feedparser article extractor
│   └── workers/
│       └── rss_worker.py     # Continuous Redis Stream consumer loop
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

**Why lifespan instead of startup/shutdown events?** The `lifespan` pattern is the modern FastAPI standard. It uses a single `async with` context, making startup and cleanup code easy to read and maintain.

```python
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.workers.request_worker import RequestWorker
from app.workers.result_worker import ResultWorker

request_worker = RequestWorker(poll_interval=2.0)
result_worker = ResultWorker(block_ms=2000, batch_size=10)
background_tasks = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Argus Orchestrator starting up on Port 8000...")
    t1 = asyncio.create_task(request_worker.start())  # Outbound dispatcher
    t2 = asyncio.create_task(result_worker.start())   # Inbound ingestor
    background_tasks.extend([t1, t2])
    yield                                              # Server runs here
    print("Argus Orchestrator shutting down...")
    request_worker.stop()
    result_worker.stop()
    await asyncio.gather(*background_tasks, return_exceptions=True)

app = FastAPI(title="Argus Orchestrator", version="2.0", lifespan=lifespan)
```

**Run with:**
```bash
uvicorn app.main:app --port 8000 --reload
```

---

### File 6: `app/api/health.py`
**Purpose:** Liveness and readiness probe endpoints for monitoring/infrastructure.

- `GET /health` — Liveness probe. Returns 200 if the process is running.
- `GET /ready` — Readiness probe. Checks both MongoDB and Redis. Returns 503 if either is down.

---

### File 7: `scripts/add_rss_request.py`
**Purpose:** Developer CLI tool to seed a `pending` crawl request into MongoDB. In production, an API endpoint would perform this insert automatically.

**What it does:**
1. Generates a unique 12-character hex correlation ID (`req_<12 hex chars>`).
2. Inserts `{"request_id": "req_...", "url": "https://...", "platform": "rss", "status": "pending"}` into `rss_requests`.
3. The Orchestrator's `request_worker` picks it up within 2 seconds and dispatches it to Redis.

```bash
# Run from argus-orchestrator:
python -m scripts.add_rss_request
```

---

### File 8: `app/schemas/request.py`
**Purpose:** Pydantic model enforcing the exact fields published to Redis Stream `queue:connector:rss`.

```python
from pydantic import BaseModel, Field

class RSSRequests(BaseModel):
    request_id: str
    url: str
    platform: str = Field(default="rss")
```

**Why validate?** Ensures no malformed task (missing URL or missing `request_id`) is ever placed on Redis. Prevents infinite retry loops from corrupt data.

---

### File 9: `app/services/request_service.py`
**Purpose:** Core outbound business logic — atomically claiming a job from MongoDB and publishing it to Redis.

**Step-by-step mechanics:**

1. **Atomic Claim** (Zero Race Conditions):
```python
claimed_doc = await rss_requests.find_one_and_update(
    {"status": "pending"},
    {"$set": {"status": "processing", "claimed_at": datetime.now(timezone.utc)}},
    return_document=ReturnDocument.AFTER
)
```
This is a **single atomic MongoDB operation**. Even with 10 workers running simultaneously, no two workers can claim the same document.

2. **Pydantic Validation:** Constructs `RSSRequests(request_id=..., url=..., platform=...)`.

3. **Redis Dispatch:**
```python
stream_message_id = await redis_client.xadd(
    settings.connector_stream,
    request_obj.model_dump()
)
```

4. **Audit Back-Link:** Saves `stream_message_id` back to MongoDB so every DB record is traceable to its Redis message.

5. **Two-Tier Error Handling:**
   - `ValidationError`: Mark as `"failed"` — prevents invalid documents from looping forever.
   - Network `Exception`: Rollback to `"pending"` — allows retry when Redis recovers.

---

### File 10: `app/workers/request_worker.py`
**Purpose:** The autonomous "Outbound Dispatcher" daemon that runs `dispatch_rss_request()` continuously.

**Key mechanics:**
1. **Pre-flight:** Tests `check_mongodb()` and `check_redis()` before entering the loop.
2. **Drain Mode:** When a job is dispatched, it sleeps only `10ms` and immediately re-polls — draining large queues at full speed.
3. **Idle Mode:** When no work is found, it waits `poll_interval` (2 seconds) using `asyncio.wait_for(self._stop_event.wait(), timeout=...)`. This prevents CPU/DB overload from empty polling.
4. **Responsive Shutdown:** `asyncio.Event` allows `Ctrl+C` to cancel the idle sleep instantly, exiting cleanly without waiting the full 2 seconds.

---

### File 11: `app/schemas/result.py`
**Purpose:** Pydantic models defining the inbound contract from the connector via Redis Stream `connector:rss:results`.

**Two Models:**
- **`RSSItem`**: A single parsed article.
  - `item_hash`: SHA-256 of `link.strip().lower()`. Used as a unique MongoDB index for deduplication. Same article scraped 100 times = only 1 database record.
  - `model_post_init()`: Automatically computes `item_hash` if not provided.
- **`CrawlResult`**: The full batch payload wrapping a list of `RSSItem`s.
  - `status`: `"success"` or `"failed"`.
  - `error_message`: Populated when `status == "failed"`.

---

### File 12: `app/services/result_service.py`
**Purpose:** Inbound business logic — saving articles to MongoDB with zero duplicates and transitioning request status.

**Key mechanics:**

1. **Index Assurance** (called once on startup):
```python
await rss_items.create_index("item_hash", unique=True)  # Deduplication
await rss_items.create_index("request_id")               # Fast lookup by job
await rss_requests.create_index("request_id", unique=True, sparse=True)
```

2. **Zero-Duplicate Bulk Upsert** (`$setOnInsert`):
```python
UpdateOne(
    {"item_hash": item.item_hash},
    {"$setOnInsert": item.model_dump()},
    upsert=True
)
```
- If the hash already exists → MongoDB skips it silently (no error, no overwrite).
- If the hash is new → MongoDB inserts the full document.
- `$setOnInsert` ensures existing articles keep their original `created_at` timestamp.
- `bulk_write(operations, ordered=False)` writes in parallel over a single network trip.

3. **State Transition:**
   - On `"success"`: Update `rss_requests` to `completed` with `items_count`, `new_items_count`, `completed_at`.
   - On `"failed"`: Update `rss_requests` to `failed` with `error_message`.

---

### File 13: `app/workers/result_worker.py`
**Purpose:** The "Inbound Ingestor" daemon — reads crawl results from Redis, saves articles, and acknowledges messages.

**Key mechanics:**
1. **Consumer Group Setup** (`xgroup_create` with `mkstream=True`): Creates both the stream and the group if they don't exist. Catches `BUSYGROUP` error if already created on restart.
2. **`xreadgroup`**: Reads messages assigned to this specific consumer. `">"` means "give me only new messages never delivered to anyone yet."
3. **Payload Flexibility**: Checks both `"payload"` (full JSON string) and `"items"` (JSON array) fields in the Redis message, since connector serializes as `{"payload": <json_string>}`.
4. **At-Least-Once Delivery**: Only calls `xack()` **after** `process_crawl_result()` successfully completes. If the server crashes before `xack`, Redis retains the message for retry.

---

## 5. argus-connector: File-by-File Walkthrough

The connector is a **stateless, database-agnostic scraping microservice**. It communicates exclusively through Redis Streams — it never connects to MongoDB.

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
    consumer_group: str = "rss_connectors"           # Consumer group (shared by all instances)
    consumer_name: str = "rss_worker-1"              # Unique ID for THIS worker instance

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore"
    )

settings = Settings()
```

> **Critical:** `result_stream` must be `"connector:rss:results"` (with the `s`). This must exactly match `result_stream` in the Orchestrator's config. A mismatch means results are published to a stream that nobody reads!

---

### Connector File 2: `app/core/logging.py`
**Purpose:** Identical logging setup to the Orchestrator so logs from both services look uniform.

```python
import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
```

---

### Connector File 3: `app/platform/redis.py`
**Purpose:** Async Redis client for the connector. Connector does NOT use MongoDB at all.

```python
import redis.asyncio as redis
from app.core.config import settings

redis_client = redis.from_url(settings.redis_url, decode_responses=True)

async def check_redis() -> bool:
    """Pings Redis to confirm broker reachability."""
    try:
        response = await redis_client.ping()
        return response is True
    except Exception:
        return False
```

---

### Connector File 4: `app/connectors/rss/model.py`
**Purpose:** Pydantic data models defining the exact contract for article data and crawl results. **Must exactly match the Orchestrator's `app/schemas/result.py`** — both services serialize and deserialize using these field names.

```python
import hashlib
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel, Field

class RSSItems(BaseModel):
    """Single parsed article. Matches Orchestrator's RSSItem exactly."""
    request_id: str
    title: str
    link: str                    # MUST be 'link' (not 'url') — Orchestrator expects 'link'
    author: Optional[str] = None # Optional: many feeds don't include author
    published_at: Optional[datetime] = None
    summary: Optional[str] = None
    item_hash: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def compute_hash(self) -> str:
        """SHA-256 of normalized link URL. Used as MongoDB deduplication key."""
        raw = self.link.strip().lower()
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def model_post_init(self, __context):
        """Auto-computes item_hash on instantiation if not already set."""
        if not self.item_hash and self.link:
            self.item_hash = self.compute_hash()

class CrawlResult(BaseModel):
    """Full crawl result payload published to 'connector:rss:results'."""
    request_id: str
    status: str = "success"       # "success" or "failed"
    items_count: int = 0          # MUST be 'items_count' (not 'item_count')
    items: List[RSSItems] = []
    error_message: Optional[str] = None
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
```

**Why `item_hash`?**
RSS feeds are scraped repeatedly (every 15 minutes). The same 10 articles will appear in the feed for days. Without hashing, MongoDB would store thousands of duplicates. The hash fingerprints each article by its URL — same URL always produces the same hash — so MongoDB's unique index silently skips any article it has already stored.

---

### Connector File 5: `app/connectors/rss/parser.py`
**Purpose:** The core feed fetcher and parser engine. Downloads a live RSS feed asynchronously and extracts structured article data.

**Technology choices:**
- **`httpx.AsyncClient`**: Asynchronous HTTP client. Non-blocking — does not freeze the event loop while waiting for the feed server to respond.
- **`feedparser`**: Universal RSS/Atom XML parser. Handles all major feed formats.
- **Realistic `User-Agent`**: Prevents Cloudflare, Akamai, or CDN blocking requests that look like bots.

```python
import feedparser
import httpx
from app.connectors.rss.model import CrawlResult, RSSItems

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ... Argus/2.0"

def _parse_published_date(entry) -> Optional[datetime]:
    """Extracts published/updated timestamp from feedparser entry and converts to UTC datetime."""
    time_struct = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if time_struct:
        try:
            return datetime.fromtimestamp(mktime(time_struct), tz=timezone.utc)
        except Exception:
            pass
    return None

async def fetch_and_parse_rss(request_id: str, url: str) -> CrawlResult:
    """
    1. Fetches raw XML from the URL using httpx (async, non-blocking).
    2. Parses XML using feedparser into structured Python objects.
    3. Extracts title, link, summary, author, published_at for each entry.
    4. Constructs RSSItems with auto-computed item_hash.
    5. Returns CrawlResult with status='success' and all items.
    6. On HTTP errors or network errors, returns CrawlResult with status='failed'.
    """
```

**Critical indentation rule learned:** The `items = []` list and the `for entry in feed.entries:` loop MUST be placed OUTSIDE the `if feed.bozo and not feed.entries:` early-return block. If indented inside it, the loop never runs and 0 items are returned even for valid feeds.

---

### Connector File 6: `app/workers/rss_worker.py`
**Purpose:** The autonomous "RSS Scraper" daemon. Consumes tasks from Redis, invokes the parser, and publishes results back.

**Full lifecycle of one task:**
1. `xreadgroup` with `">"` pulls one new undelivered task from `queue:connector:rss`.
2. Extracts `request_id` and `url` from the Redis message fields.
3. Calls `fetch_and_parse_rss(request_id, url)` which returns a `CrawlResult`.
4. Serializes the result: `crawl_result.model_dump_json()`.
5. Publishes to results stream: `redis_client.xadd(result_stream, {"payload": payload_json})`.
6. Acknowledges original task: `redis_client.xack(connector_stream, group_name, message_id)`.

**Note:** `xack` is only called AFTER publishing the result. If the connector crashes between step 5 and 6, Redis retains the original task for retry.

```python
class RSSWorker:
    def __init__(self, block_ms: int = 2000, batch_size: int = 5):
        # block_ms: how long xreadgroup waits for new messages before returning empty
        # batch_size: max messages to read per loop iteration
```

**Key indentation bugs to remember:**
- `run_worker()` must be a **module-level** function (0 indentation), NOT inside `class RSSWorker`.
- `def stop(self)` must be a method of `RSSWorker` (4-space indent), NOT inside `_process_task`.
- The `for stream_key, messages in entries:` loop must be OUTSIDE the `if not entries: continue` block.
- `xgroup_create` uses `groupname=...` (NOT `group=...`).

---

### Connector File 7: `app/main.py`
**Purpose:** FastAPI entrypoint for the connector service. Manages the `RSSWorker` lifecycle via `lifespan`.

```python
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.workers.rss_worker import RSSWorker

worker = RSSWorker(block_ms=2000, batch_size=5)
worker_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Argus Connector starting up on port 8001...")
    worker_task = asyncio.create_task(worker.start())  # Starts background scraper
    yield                                               # Server runs here
    print("Argus Connector shutting down...")
    worker.stop()
    await asyncio.gather(worker_task, return_exceptions=True)

app = FastAPI(title="Argus Connector", version="2.0", lifespan=lifespan)

@app.get("/health")
async def health():
    redis_ok = await check_redis()
    return {"status": "healthy" if redis_ok else "unhealthy", "redis": "connected" if redis_ok else "disconnected"}
```

**Run with:**
```bash
uvicorn app.main:app --port 8001 --reload
```

---

## 6. End-to-End Pipeline: How to Run the Full System

### Step 1: Start Orchestrator (Port 8000)
```bash
# In argus-orchestrator directory:
uvicorn app.main:app --port 8000 --reload
```

### Step 2: Start Connector (Port 8001)
```bash
# In argus-connector directory:
uvicorn app.main:app --port 8001 --reload
```

### Step 3: Seed a Test Job
```bash
# In a 3rd terminal, in argus-orchestrator directory:
python -m scripts.add_rss_request
```

### What Happens (In Order):
1. `add_rss_request.py` inserts a `{status: "pending"}` document into MongoDB `rss_requests`.
2. Orchestrator's `request_worker` detects it within 2 seconds, marks it `processing`, pushes to Redis `queue:connector:rss`.
3. Connector's `rss_worker` instantly picks it up via `xreadgroup`.
4. `fetch_and_parse_rss()` downloads the live RSS XML using `httpx` and parses articles with `feedparser`.
5. Connector publishes `CrawlResult` JSON to Redis `connector:rss:results` and calls `xack`.
6. Orchestrator's `result_worker` picks up the result, calls `result_service.process_crawl_result()`.
7. Articles bulk-upserted into MongoDB `rss_items` (zero duplicates via `item_hash` unique index).
8. MongoDB `rss_requests` document updated: `status: "completed"`, `items_count`, `new_items_count`, `completed_at`.

### Health Check URLs:
- `http://localhost:8000/` — Orchestrator service identity
- `http://localhost:8000/ready` — MongoDB + Redis readiness probe
- `http://localhost:8001/` — Connector service identity
- `http://localhost:8001/health` — Redis health probe
- `http://localhost:8000/docs` — Orchestrator Swagger UI
- `http://localhost:8001/docs` — Connector Swagger UI

---

## 7. Bugs Encountered & Fixes Applied

### Bug 1: `ModuleNotFoundError: No module named 'motor'`
**Where:** Orchestrator startup
**Fix:** Installed `motor>=3.7.0` in virtual environment, added to `requirements.txt`.

### Bug 2: Pydantic `extra_forbidden` crash on startup
**Where:** `app/core/config.py` — `.env` had `RESULT_STREAM` but Settings didn't declare it.
**Fix:** Added `result_stream: str` field and `extra="ignore"` to `SettingsConfigDict`.

### Bug 3: `commad` typo in MongoDB ping
**Where:** `app/platform/mongodb.py`
**Fix:** `mongo_client.admin.commad("ping")` → `mongo_client.admin.command("ping")`.

### Bug 4: `decode_response = True` (wrong spelling)
**Where:** `app/platform/redis.py`
**Fix:** `decode_response` → `decode_responses` (with `s`).

### Bug 5: `url_has_1` duplicate index collision in MongoDB
**Symptom:** `DuplicateKeyError` when seeding requests.
**Root Cause:** Old malformed unique index without `sparse=True` existed from prior testing.
**Fix:** Dropped the old index `url_has_1` manually via MongoDB Compass.

### Bug 6: `AttributeError: 'RSSItems' object has no attribute 'link'`
**Where:** `argus-connector/app/connectors/rss/model.py`
**Root Cause:** Field declared as `url: str` but `compute_hash()` referenced `self.link`.
**Fix:** Renamed field from `url` to `link` throughout model, parser, and test blocks.

### Bug 7: Connector sent `url` in JSON but Orchestrator expected `link`
**Symptom:** `ValidationError: 10 validation errors for CrawlResult — items.N.link: Field required`
**Root Cause:** Connector model used `url: str`; `model_dump_json()` serialized it as `"url"`. Orchestrator's `RSSItem` expects `"link"`.
**Fix:** Renamed field to `link: str` in connector's `model.py` and all references in `parser.py`.

### Bug 8: `result_stream` name mismatch between services
**Symptom:** Connector published results; Orchestrator's `result_worker` never received them. Requests stayed stuck in `processing` forever.
**Root Cause:** Connector's `.env` had `RESULT_STREAM=connector:rss:result` (missing the `s`). Orchestrator listened on `connector:rss:results`.
**Fix:** Updated connector `.env` and `config.py` default to `connector:rss:results`.

### Bug 9: `items = []` and `for entry in feed.entries:` indented inside bozo error block
**Symptom:** Parser returned `items_count: 0` even when feed had 10 articles.
**Root Cause:** After `return CrawlResult(status="failed", ...)`, the item extraction loop was indented further inside the `if` block. Python's `continue`/`return` in the outer block meant the loop body was dead code.
**Fix:** Moved `items = []` and the `for` loop to the correct outer indentation level.

### Bug 10: `run_worker()` indented inside `class RSSWorker`
**Symptom:** `ImportError: cannot import name 'run_worker' from 'app.workers.rss_worker'`
**Root Cause:** `async def run_worker()` had 4 spaces of indentation, making it a method of the class, not a module-level function.
**Fix:** Moved `run_worker()` to 0-space indentation, outside the class.

### Bug 11: `xgroup_create(group=...)` — wrong keyword argument
**Symptom:** `NOGROUP` error on every `xreadgroup` call in a crash loop.
**Root Cause:** `_setup_consumer_group` used `group=self.group_name` but the correct argument is `groupname=self.group_name`.
**Fix:** Changed `group` → `groupname`.

### Bug 12: `for stream_key, messages` loop under `if not entries: continue`
**Symptom:** Worker received messages from Redis but never processed them.
**Root Cause:** The processing `for` loop was indented inside `if not entries: continue`, making it dead code when entries were present.
**Fix:** Moved the `for` loop to the correct outer indentation, after the `if not entries: continue` guard.

---

## 8. Current Roadmap & Project Progress

```
[x] Phase 1:  Environment & Async Database Drivers (Motor & Redis)
[x] Phase 2:  Configuration & Logging Setup
[x] Phase 3:  Outbound Request Schema (app/schemas/request.py)
[x] Phase 4:  Atomic Request Dispatch Service (app/services/request_service.py)
[x] Phase 5:  Outbound Request Worker Daemon (app/workers/request_worker.py)
[x] Phase 6:  Inbound Result Schema (app/schemas/result.py)
[x] Phase 7:  Result Ingestion Service (app/services/result_service.py)
[x] Phase 8:  Inbound Result Worker Consumer (app/workers/result_worker.py)
[x] Phase 9:  FastAPI Lifespan Integration (app/main.py - both services on Uvicorn)
[x] Phase 10: argus-connector - RSS Connector Implementation
              - app/core/config.py (connector settings)
              - app/core/logging.py
              - app/platform/redis.py
              - app/connectors/rss/model.py (RSSItems + CrawlResult)
              - app/connectors/rss/parser.py (httpx + feedparser)
              - app/workers/rss_worker.py (Redis consumer daemon)
              - app/main.py (FastAPI lifespan, /health endpoint)
[x] Phase 11: End-to-End Pipeline Verified (Orchestrator <-> Redis <-> Connector <-> MongoDB)
──────────────────────────────────────────────────────────────────────────────────────────
[ ] Phase 12: REST API Endpoint (POST /api/feeds) to replace add_rss_request.py seed script
[ ] Phase 13: NSE Announcements Connector (argus-connector/app/connectors/nse/)
[ ] Phase 14: BSE Announcements Connector
[ ] Phase 15: SEBI Orders & Circulars Connector (HTML scraper + PDF link extractor)
[ ] Phase 16: SAT (Securities Appellate Tribunal) Orders Connector
[ ] Phase 17: Stocktwits Social Feed Connector (REST API + JSON)
[ ] Phase 18: Bing News RSS Connector (custom RSS feeds per ticker/keyword)
[ ] Phase 19: Scheduler Integration (APScheduler for automatic periodic re-crawls)
[ ] Phase 20: Unified Data Model (single 'market_intelligence_items' collection)
```


---

## Table of Contents
1. [The System Architecture: What are we building?](#1-the-system-architecture-what-are-we-building)
2. [The Request State Lifecycle](#2-the-request-state-lifecycle)
3. [Repository Directory Structure](#3-repository-directory-structure)
4. [Master File-by-File Walkthrough & Code Explanation](#4-master-file-by-file-walkthrough--code-explanation)
   - [Config: `app/core/config.py`](#file-1-appcoreconfigpy---application-configuration)
   - [Logging: `app/core/logging.py`](#file-2-appcoreloggingpy---centralized-logger-formatting)
   - [MongoDB Client: `app/platform/mongodb.py`](#file-3-appplatformmongodbpy---asynchronous-database-layer)
   - [Redis Client: `app/platform/redis.py`](#file-4-appplatformredispy---asynchronous-message-broker)
   - [FastAPI & Health: `app/api/health.py` & `app/main.py`](#file-5-appapihealthpy--appmainpy---web-service--readiness-probes)
   - [Seed Script: `scripts/add_rss_request.py`](#file-6-scriptsadd_rss_requestpy---job-creation-seed-script)
   - [Outbound Schema: `app/schemas/request.py`](#file-7-appschemasrequestpy---outbound-request-data-contract)
   - [Outbound Service: `app/services/request_service.py`](#file-8-appservicesrequest_servicepy---atomic-dispatch-engine)
   - [Outbound Worker: `app/workers/request_worker.py`](#file-9-appworkersrequest_workerpy---continuous-polling-worker)
   - [Inbound Schema: `app/schemas/result.py`](#file-10-appschemasresultpy---inbound-article--job-result-contract)
   - [Inbound Service: `app/services/result_service.py`](#file-11-appservicesresult_servicepy---article-storage--request-completion-upcoming)
   - [Inbound Worker: `app/workers/result_worker.py`](#file-12-appworkersresult_workerpy---redis-result-consumer-upcoming)
5. [Current Roadmap & Project Progress](#5-current-roadmap--project-progress)

---

## 1. The System Architecture: What are we building?

Argus 2.0 is an **event-driven, decoupled web intelligence and content ingestion system**. 

Instead of building a monolithic script that downloads feeds, parses articles, and saves them all in one place, the system is separated into two independent microservices communicating through **Redis Streams**:

```
┌────────────────────────────────────────────────────────────────────────┐
│                          ARGUS ORCHESTRATOR                            │
│                                                                        │
│   [MongoDB: rss_requests]                                              │
│          │ (1. Atomic Claim: status -> 'processing')                   │
│          ▼                                                             │
│   [request_worker.py] ──> [request_service.py]                         │
│                                  │                                     │
│                                  │ (2. xadd: Outbound Task)            │
└──────────────────────────────────┼─────────────────────────────────────┘
                                   │
                                   ▼
          ┌──────────────────────────────────────────────────┐
          │  Redis Stream: 'queue:connector:rss'             │
          └──────────────────────────────────────────────────┘
                                   │
                                   ▼ (3. Pull Job via xreadgroup)
┌────────────────────────────────────────────────────────────────────────┐
│                           ARGUS CONNECTOR                              │
│                                                                        │
│   [rss_worker.py] ──> [parser.py]                                      │
│                             │                                          │
│                             │ (Fetches XML from internet & extracts)   │
│                             ▼                                          │
│                       [Parsed Articles]                                │
│                             │                                          │
│                             │ (4. xadd: Inbound Results)               │
└─────────────────────────────┼──────────────────────────────────────────┘
                              │
                              ▼
          ┌──────────────────────────────────────────────────┐
          │  Redis Stream: 'connector:rss:results'           │
          └──────────────────────────────────────────────────┘
                              │
                              ▼ (5. Consume Results)
┌─────────────────────────────┼──────────────────────────────────────────┐
│                             ▼                                          │
│   [result_worker.py] ──> [result_service.py]                           │
│                                  │                                     │
│                                  ├──> [MongoDB: rss_items] (Articles)  │
│                                  │                                     │
│                                  └──> [MongoDB: rss_requests]          │
│                                       (status -> 'completed')          │
│                          ARGUS ORCHESTRATOR                            │
└────────────────────────────────────────────────────────────────────────┘
```

### Why This Architecture?
1. **Zero Bottlenecks:** The Orchestrator never slows down waiting for slow websites to respond.
2. **Infinite Scaling:** If 10,000 feeds need to be crawled, you can spin up 20 instances of `argus-connector` across multiple machines without modifying a single line of Orchestrator code.
3. **Fault Tolerance:** If a connector worker crashes while fetching a feed, Redis and MongoDB guarantee the task is preserved and retried.

---

## 2. The Request State Lifecycle

Every feed crawl task in MongoDB's `rss_requests` collection travels through a strict, auditable state machine:

```
                  ┌──────────────┐
                  │   pending    │  <-- Job seeded in MongoDB
                  └──────┬───────┘
                         │
                         │ (Atomically claimed by request_service.py)
                         ▼
                  ┌──────────────┐
                  │  processing  │  <-- Dispatched to Redis Stream ('queue:connector:rss')
                  └──────┬───────┘
                         │
           ┌─────────────┴─────────────┐
           │ (Connector succeeds)      │ (Connector errors or invalid URL)
           ▼                           ▼
    ┌──────────────┐            ┌──────────────┐
    │  completed   │            │    failed    │
    └──────────────┘            └──────────────┘
```

1. **`pending`**: The request exists in MongoDB but has not been picked up yet.
2. **`processing`**: An orchestrator worker atomically claimed the job, recorded `claimed_at`, and pushed the task to Redis Stream `queue:connector:rss` (storing `stream_message_id`).
3. **`completed`**: The connector finished fetching the feed, returned the articles, and the orchestrator saved them into `rss_items`.
4. **`failed`**: The URL was unreachable, timed out, or returned an invalid RSS feed. The reason is recorded in `error_message`.

---

## 3. Repository Directory Structure

```text
argus-orchestrator/
├── .env                      # Local environment overrides (passwords, URLs)
├── .env.example              # Template environment variables for setup
├── requirements.txt          # Python library dependencies
├── PROJECT_GUIDE.md          # THIS MASTER FILE: Full architecture & code guide
├── example.txt               # Engineering log & progress notes
├── app/
│   ├── main.py               # FastAPI application entrypoint & readiness endpoints
│   ├── api/
│   │   └── health.py         # Health check (/health) and database readiness (/ready)
│   ├── core/
│   │   ├── config.py         # Pydantic Settings configuration loader
│   │   └── logging.py        # Centralized logger configuration
│   ├── platform/
│   │   ├── mongodb.py        # Async MongoDB connection pool (Motor) & collections
│   │   └── redis.py          # Async Redis connection pool (redis-py) & ping checks
│   ├── schemas/
│   │   ├── request.py        # Outbound Pydantic schema (sent to Redis queue)
│   │   └── result.py         # Inbound Pydantic schema (received from Redis results)
│   ├── services/
│   │   ├── request_service.py# Core outbound business logic (atomic claim & xadd)
│   │   └── result_service.py # Core inbound business logic (save articles & complete job)
│   └── workers/
│       ├── request_worker.py # Continuous background loop polling MongoDB for pending jobs
│       └── result_worker.py  # Continuous background consumer reading results from Redis
└── scripts/
    └── add_rss_request.py    # CLI tool to seed test pending requests into MongoDB
```

---

## 4. Master File-by-File Walkthrough & Code Explanation

---

### File 1: `app/core/config.py` - Application Configuration
* **Purpose:** Single source of truth for all configuration values. It reads values from `.env` and provides strongly-typed settings across the entire app.
* **Why Pydantic Settings?** It automatically validates types (e.g. integer ports, valid strings) and prevents hardcoded passwords or URLs in code.

```python
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_name: str = "argus-orchestrator"
    app_env: str = "development"
    mongo_url: str = "mongodb://localhost:27017"      # MongoDB host and port
    mongo_database: str = "argus"                     # Database name
    redis_url: str = "redis://localhost:6379/0"       # Redis host, port, and DB index
    connector_stream: str = "queue:connector:rss"     # Redis stream where we PUSH jobs
    result_stream: str = "connector:rss:results"      # Redis stream where workers RETURN articles
    consumer_group: str = "orchestrator-results"      # Consumer group name for reading results
    consumer_name: str = "orchestrator-1"             # Unique identifier for this instance

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore"                                # Prevents crashes if extra env vars exist
    )

settings = Settings()
```

---

### File 2: `app/core/logging.py` - Centralized Logger Formatting
* **Purpose:** Standardizes the log output across all workers and services so timestamps, log levels, module names, and messages are formatted consistently.

```python
import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s |"
    )
```

---

### File 3: `app/platform/mongodb.py` - Asynchronous Database Layer
* **Purpose:** Sets up the asynchronous MongoDB client using `motor.motor_asyncio` and defines handles for the two primary collections: `rss_requests` and `rss_items`.
* **Key Concept:** `motor` uses Python `asyncio` under the hood. When database queries run, the Python process does not freeze; it can continue processing other tasks concurrently.

```python
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from app.core.config import settings

logger = logging.getLogger(__name__)

# Initialize the async client using configured URL
mongo_client = AsyncIOMotorClient(settings.mongo_url)

# Select the target database ('argus')
database = mongo_client[settings.mongo_database]

# Primary Collection 1: Stores job requests and status tracking
rss_requests = database["rss_requests"]

# Primary Collection 2: Stores downloaded and parsed article items
rss_items = database["rss_items"]

async def check_mongodb():
    """Pings MongoDB using the admin command to verify the database is alive."""
    try:
        await mongo_client.admin.command("ping")
        return True
    except Exception as exc:
        logger.exception("MongoDB connection failed")
        return False
```

---

### File 4: `app/platform/redis.py` - Asynchronous Message Broker
* **Purpose:** Sets up the async Redis connection pool using `redis.asyncio`.
* **Key Setting:** `decode_responses=True` tells Redis to automatically decode binary bytes into standard Python UTF-8 strings, avoiding ugly `b'...'` byte conversions throughout the codebase.

```python
import logging
import redis.asyncio as redis
from app.core.config import settings

logger = logging.getLogger(__name__)

# Connect to Redis with string decoding enabled
redis_client = redis.from_url(settings.redis_url, decode_responses=True)

async def check_redis():
    """Pings Redis to confirm reachability."""
    try:
        response = await redis_client.ping()
        return response is True
    except Exception as exc:
        logger.exception("Redis connection failed")
        return False
```

---

### File 5: `app/api/health.py` & `app/main.py` - Web Service & Readiness Probes
* **Purpose:** Provides a lightweight FastAPI application for Docker, Kubernetes, or load balancers to monitor the health and readiness of the Orchestrator service.
* **Endpoints:**
  - `GET /`: Basic service identifier.
  - `GET /health`: Liveness probe (returns 200 OK if the process is up).
  - `GET /ready`: Readiness probe (checks both MongoDB and Redis before returning 200 OK; returns 503 if either database is down).

---

### File 6: `scripts/add_rss_request.py` - Job Creation Seed Script
* **Purpose:** Simulates how an API or user adds a new feed to be monitored into the system.
* **Code Explanation:**
  - Generates a unique correlation ID (`request_id: "req_<12 hex chars>"`).
  - Specifies the RSS feed URL.
  - Sets `"status": "pending"` so the dispatcher worker knows to pick it up.
  - Inserts the document into MongoDB collection `rss_requests`.

---

### File 7: `app/schemas/request.py` - Outbound Request Data Contract
* **Purpose:** The Pydantic model enforcing the exact fields that are packaged and published to Redis Stream `queue:connector:rss`.
* **Code:**
```python
from pydantic import BaseModel, Field

class RSSRequests(BaseModel):
    request_id: str
    url: str
    platform: str = Field(default="rss")
```
* **Why:** Ensures that no malformed task (e.g., missing URL or missing `request_id`) is ever transmitted onto Redis.

---

### File 8: `app/services/request_service.py` - Atomic Dispatch Engine
* **Purpose:** Contains the core business logic of claiming a pending request and pushing it onto the Redis stream.
* **Line-by-Line Mechanics:**
  1. **Atomic Claiming:** Uses `rss_requests.find_one_and_update({"status": "pending"}, {"$set": {"status": "processing", "claimed_at": ...}})` with `ReturnDocument.AFTER`. This guarantees that even with 10 workers running concurrently, no two workers can claim the same job.
  2. **Empty Check:** If no `pending` jobs exist, returns `None` immediately.
  3. **Validation:** Validates against `RSSRequests` schema.
  4. **Redis Dispatch:** Calls `await redis_client.xadd(settings.connector_stream, request_obj.model_dump())` to publish the message to `queue:connector:rss`.
  5. **Audit Trail Back-Link:** Updates the MongoDB document with `stream_message_id` returned by Redis.
  6. **Two-Tier Error Handling:**
     - If schema validation fails (`ValidationError`), marks the document as `"failed"` with the error description so invalid URLs do not loop forever.
     - If a transient network error occurs (e.g. Redis temporarily unreachable), rolls the status back to `"pending"` so it can be retried automatically.

---

### File 9: `app/workers/request_worker.py` - Continuous Polling Worker
* **Purpose:** The background daemon (The "Outbound Dispatcher") that continuously runs `dispatch_rss_request()` in an infinite loop.
* **Line-by-Line Mechanics:**
  1. **Pre-flight Check:** Tests `check_mongodb()` and `check_redis()` before entering the loop.
  2. **Drain Mode vs. Idle Mode:**
     - When a job is dispatched, it yields for only `10ms` (`asyncio.sleep(0.01)`) and immediately loops again, draining large queues instantly.
     - When no work is available, it pauses for `poll_interval` (2 seconds) using `asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)`.
  3. **Instant Responsive Shutdown:** Uses `asyncio.Event` (`self._stop_event`). Calling `worker.stop()` cancels the sleep instantly without hanging for 2 seconds.
  4. **Signal Handling:** Catches `Ctrl+C` (`KeyboardInterrupt`) on Windows and `SIGTERM`/`SIGINT` on Linux to shut down cleanly without corrupting in-flight tasks.

---

### File 10: `app/schemas/result.py` - Inbound Article & Job Result Contract
* **Purpose:** The Pydantic model defining the structure of articles and crawl results sent back from the connector worker over Redis Stream `connector:rss:results`.
* **Two Core Models:**
  - **`RSSItem`**: A single parsed article (`request_id`, `title`, `link`, `summary`, `author`, `published_at`, `item_hash`, `created_at`).
    - `compute_hash()`: Generates a SHA-256 hash of the lowercased URL.
    - `model_post_init()`: Automatically computes and populates `item_hash` if not explicitly supplied.
  - **`CrawlResult`**: The overall batch response for a job (`request_id`, `status: "success"` or `"failed"`, `items_count`, `items: List[RSSItem]`, `error_message`, `fetched_at`).
* **Executable Self-Test:** Run `python -m app.schemas.result` to verify validation and hashing.

---

### File 11: `app/services/result_service.py` - Article Storage & Request Completion
* **Purpose:** The inbound business logic engine. Ingests `CrawlResult` payloads received from the connector, batch-upserts articles into MongoDB with zero duplicates, transitions request status to `"completed"` or `"failed"`, and ensures high-performance indexes.
* **Key Architecture & Mechanics:**
  1. **Index Assurance (`ensure_indexes`)**:
     - Creates a unique index on `rss_items.item_hash` (guaranteeing article uniqueness at the database level).
     - Creates index on `rss_items.request_id` and `rss_items.published_at` for rapid querying.
     - Creates index on `rss_requests.request_id` for instantaneous status transitions.
  2. **Zero-Duplicate Bulk Upsert (`save_rss_items`)**:
     - Uses `pymongo.UpdateOne` with `$setOnInsert` and `upsert=True`:
       ```python
       UpdateOne(
           {"item_hash": item.item_hash},
           {"$setOnInsert": item.model_dump()},
           upsert=True
       )
       ```
     - **Why `$setOnInsert`?** If an article already exists from a previous crawl, its original `created_at` timestamp is preserved rather than overwritten.
     - Uses `bulk_write(operations, ordered=False)` so MongoDB can write in parallel over a single network trip.
  3. **State Machine Transition (`process_crawl_result`)**:
     - Validates payload against `CrawlResult`.
     - **If `status == "success"`**: Saves articles, updates `rss_requests` (`status: "completed"`, `items_count`, `new_items_count`, `completed_at`).
     - **If `status == "failed"`**: Updates `rss_requests` (`status: "failed"`, `error_message`, `completed_at`).
  4. **Executable Self-Test**:
     - Run `python -m app.services.result_service` to test index generation, article insertion, and deduplication verification.

---

### File 12: `app/workers/result_worker.py` - Redis Result Consumer Daemon
* **Purpose:** The continuous background daemon (The "Inbound Ingestor"):
  - Connects to Redis Stream `connector:rss:results` using consumer group `orchestrator-results`.
  - Reads new result messages in batches using `xreadgroup()`.
  - Deserializes JSON payloads and passes results to `process_crawl_result()` in `result_service.py` to save articles to MongoDB.
  - Sends `xack()` to Redis upon successful persistence so messages are never dropped or reprocessed.
* **Key Mechanics:**
  1. **Pre-flight Health**: Verifies MongoDB & Redis availability before starting.
  2. **Automatic Group Creation (`_setup_consumer_group`)**: Uses `xgroup_create` with `mkstream=True`, catching `BUSYGROUP` if already created.
  3. **Payload Flexibility**: Automatically detects and decodes JSON-serialized article strings (`items` or `payload` fields) sent by connector workers.
  4. **At-Least-Once Delivery**: Only calls `xack()` after `process_crawl_result()` successfully completes, ensuring Redis retains messages if a crash occurs.
  5. **Signal & Stop Handling**: Responsive shutdown via `asyncio.Event` and signal handlers (`SIGINT`, `SIGTERM`).

---

## 5. Current Roadmap & Project Progress

```
[x] Phase 1: Environment & Async Database Drivers (Motor & Redis)
[x] Phase 2: Configuration & Logging Setup
[x] Phase 3: Outbound Request Schema (app/schemas/request.py)
[x] Phase 4: Atomic Request Dispatch Service (app/services/request_service.py)
[x] Phase 5: Outbound Request Worker Daemon (app/workers/request_worker.py)
[x] Phase 6: Inbound Result Schema (app/schemas/result.py)
[x] Phase 7: Result Ingestion Service (app/services/result_service.py)
[x] Phase 8: Inbound Result Worker Consumer (app/workers/result_worker.py)
─────────────────────────────────────────────────────────────────────────────
[ ] Phase 9: Connector Worker Implementation (argus-connector repo) <-- CURRENT STEP
```


