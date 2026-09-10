# Argus 2.0 Orchestrator: Master Project Guide & Code Walkthrough

This document is the definitive, step-by-step master guide for the **Argus 2.0 Orchestrator**. It documents every architectural decision, every directory, and every single file—explaining its exact purpose, what its code does line-by-line, and how each piece fits into the overall system.

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


