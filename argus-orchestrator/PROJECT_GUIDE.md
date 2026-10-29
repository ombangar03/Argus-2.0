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
   - [Inbound Worker & XAUTOCLAIM Recovery: `app/workers/result_worker.py`](#file-13-appworkersresult_workerpy)
   - [News Query Schema: `app/schemas/query.py`](#file-14-appschemasquerypy)
   - [Bing URL Builder: `app/services/bing_service.py`](#file-15-appservicesbing_servicepy)
   - [Seed News Queries: `scripts/seed_news_query.py`](#file-16-scriptsseed_news_querypy)
   - [Bing Test Script: `app/test/test_bing.py`](#file-17-apptesttest_bingpy)
   - [Query Dispatch Service: `app/services/query_dispatch_service.py`](#file-18-appservicesquery_dispatch_servicepy)
   - [Request Idempotency Test: `app/test/test_request_idempotency.py`](#file-19-apptesttest_request_idempotencypy)
   - [Schedule Time Helper: `app/services/schedule_time_service.py`](#file-20-appservicesschedule_time_servicepy)
   - [Scheduled Request Service: `app/services/scheduled_request_service.py`](#file-21-appservicesscheduled_request_servicepy)
   - [Scheduler Service: `app/services/scheduler_service.py`](#file-22-appservicesscheduler_servicepy)
   - [Scheduler Worker: `app/workers/scheduler_worker.py`](#file-23-appworkersscheduler_workerpy)
   - [Schedule Time Service Test: `app/test/test_schedule_time_service.py`](#file-24-apptesttest_schedule_time_servicepy)
   - [Scheduled Request Service Test: `app/test/test_scheduled_request_service.py`](#file-25-apptesttest_scheduled_request_servicepy)
   - [Scheduler Dispatch Test: `app/test/test_scheduler_dispatch.py`](#file-26-apptesttest_scheduler_dispatchpy)
   - [Scheduler Sync Test: `app/test/test_scheduler_sync.py`](#file-27-apptesttest_scheduler_syncpy)
   - [Scheduler Sync Due-Time Test: `app/test/test_scheduler_sync_due_time.py`](#file-28-apptesttest_scheduler_sync_due_timepy)
   - [Scheduler Tick Test: `app/test/test_scheduler_tick.py`](#file-29-apptesttest_scheduler_tickpy)
   - [Scheduler Worker Lifecycle Test: `app/test/test_scheduler_worker.py`](#file-30-apptesttest_scheduler_workerpy)
   - [Scheduler Service Manual Test: `app/test/test_scheduler_service.py`](#file-31-apptesttest_scheduler_servicepy)
   - [Result Worker Unit Test: `app/test/test_result_worker.py`](#file-32-apptesttest_result_workerpy)
   - [Result Worker Manual Recovery Test: `app/test/manual_result_worker_recovery.py`](#file-33-apptestmanual_result_worker_recoverypy)
   - [Result Service Unit Tests: `app/test/test_result_service.py`](#file-34-apptesttest_result_servicepy)
   - [Request Service Unit Tests: `app/test/test_request_service.py`](#file-35-apptesttest_request_servicepy)
   - [Redis Dispatch Stream Recovery Regression Test: `app/test/test_redis_dispatch_recovery.py`](#file-36-apptesttest_redis_dispatch_recoverypy)
   - [Stale Request Recovery Tests: `app/test/test_request_recovery.py`](#file-37-apptesttest_request_recoverypy)
   - [Request Worker Unit Tests: `app/test/test_request_worker.py`](#file-38-apptesttest_request_workerpy)
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
   - [Generic Connector Worker & XAUTOCLAIM Recovery: `app/workers/connector_worker.py`](#connector-file-10-appworkersconnector_workerpy)
   - [FastAPI Entrypoint: `app/main.py`](#connector-file-11-appmainpy)
   - [Connector Test Suite: `app/test/`](#connector-file-12-apptest)
   - [NSE Announcements Models: `app/connectors/nse/models.py`](#connector-file-13-appconnectorsnsemodelspy)
   - [NSE XML Parser: `app/connectors/nse/parser.py`](#connector-file-14-appconnectorsnseparserpy)
   - [NSE HTTP Client: `app/connectors/nse/rss.py`](#connector-file-15-appconnectorsnsersspy)
   - [NSE Connector: `app/connectors/nse/connector.py`](#connector-file-16-appconnectorsnseconnectorpy)
   - [NSE Circulars API Client: `app/connectors/nse/circulars.py`](#connector-file-17-appconnectorsnsecircularspy)
6. [End-to-End Pipeline: How to Run the Full System](#6-end-to-end-pipeline-how-to-run-the-full-system)
7. [Bugs Encountered & Fixes Applied](#7-bugs-encountered--fixes-applied)
   - [Bug 22: Redis Dispatch Deadlock on Stream Truncation / Message Eviction](#bug-22-redis-dispatch-deadlock-on-stream-truncation--message-eviction)
   - [Bug 23: `AttributeError` on Redis ResponseError in Result Worker](#bug-23-attributeerror-on-redis-responseerror-in-result-worker)
   - [Bug 24: Unbounded MongoDB Duplicate Write Amplification & Multi-Layer Redis Deduplication Cache](#bug-24-unbounded-mongodb-duplicate-write-amplification--multi-layer-redis-deduplication-cache)
8. [Current Roadmap & Project Progress](#8-current-roadmap--project-progress)
9. [Implementation Guide: Redis Scheduler, Daily Scheduling & Ingest Recovery](#9-implementation-guide-redis-scheduler-daily-scheduling--ingest-recovery)
   - [9.1 System Architecture & End-to-End Pipeline](#91-system-architecture--end-to-end-pipeline)
   - [9.2 Scheduler Design & Redis Hot-Path Data Structures](#92-scheduler-design--redis-hot-path-data-structures)
   - [9.3 Two-Phase Dispatch & Compare-And-Swap (CAS) Lua](#93-two-phase-dispatch--compare-and-swap-cas-lua)
   - [9.4 Daily Scheduling & Timezone Calculations](#94-daily-scheduling--timezone-calculations)
   - [9.5 MongoDB-to-Redis Synchronization & Reconciliation](#95-mongodb-to-redis-synchronization--reconciliation)
   - [9.6 Inbound Result Worker & `XAUTOCLAIM` Recovery](#96-inbound-result-worker--xautoclaim-recovery)
   - [9.7 Comprehensive Test Verification Matrix](#97-comprehensive-test-verification-matrix)
   - [9.8 Resolution of Prototype Defects (Register 1–8)](#98-resolution-of-prototype-defects-register-18)
   - [9.9 Outstanding Work, Verification Gaps & Production Readiness](#99-outstanding-work-verification-gaps--production-readiness)
   - [9.10 Redis Article Deduplication Architecture & Stream Recovery Semantics](#910-redis-article-deduplication-architecture--stream-recovery-semantics)
10. [Implementation Guide: Stale Request Recovery, Idempotent Dispatch & Worker Reconciliation](#10-implementation-guide-stale-request-recovery-idempotent-dispatch--worker-reconciliation)
    - [10.1 Request Dispatch and Recovery Architecture](#101-request-dispatch-and-recovery-architecture)
    - [10.2 Idempotent Redis Publishing & Deduplication Keys](#102-idempotent-redis-publishing--deduplication-keys)
    - [10.3 Stale Request Recovery Implementation (`recover_stale_processing_requests`)](#103-stale-request-recovery-implementation-recover_stale_processing_requests)
    - [10.4 Request Worker Integration](#104-request-worker-integration)
    - [10.5 Configuration & Pydantic Validation](#105-configuration--pydantic-validation)
    - [10.6 Tests and Current Verification Status](#106-tests-and-current-verification-status)
    - [10.7 Remaining Work & Open Review Items](#107-remaining-work--open-review-items)

---

## 1. System Architecture: What Are We Building?

Argus 2.0 is an **event-driven, decoupled web intelligence and content ingestion system**.

Instead of building a monolithic script that downloads feeds, parses articles, and saves them in one place, the system is split into two independent microservices communicating exclusively through **Redis Streams**, with **MongoDB** acting as the durable system of record:

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                    ARGUS ORCHESTRATOR  (Port 8000)                                     │
│                                                                                        │
│   [MongoDB: scheduled_units]                                                           │
│           │                                                                            │
│           │ (1. sync_scheduled_units_from_mongodb: sync & reconcile)                   │
│           ▼                                                                            │
│   [Redis Hot Path: schedule:due:{platform}:{priority} ZSET & unit:{unit_id} HASH]      │
│           │                                                                            │
│           │ (2. dispatch_due_units_once: due check score <= now)                       │
│           ▼                                                                            │
│   [scheduler_worker.py]                                                                │
│           │                                                                            │
│           │ (3. Step A: create_scheduled_rss_request -> MongoDB rss_requests 'pending')│
│           │     Step B: Lua CAS _ADVANCE_DUE_TIME_SCRIPT advances Redis due time       │
│           ▼                                                                            │
│   [MongoDB: rss_requests]  <───────────── (Manual / Query Seeding: seed_rss_request)   │
│           │                                                                            │
│           │ (4. Atomic Claim: status -> 'processing', claimed_at = now)                │
│           ▼                                                                            │
│   [request_worker.py] ──> [request_service.py: dispatch_request]                       │
│                                   │                                                    │
│                                   │ (5. Idempotent XADD Lua Script)                    │
│                                   │     Dedupe Key: argus:dispatch:request:...         │
│                                   ▼                                                    │
└───────────────────────────────────┼────────────────────────────────────────────────────┘
                                    │
                                    ▼
           ┌──────────────────────────────────────────────────┐
           │  Redis Stream: 'queue:connector:rss'             │
           └──────────────────────────────────────────────────┘
                                    │
                                    ▼ (6. Pull via xreadgroup; retry via xautoclaim)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                    ARGUS CONNECTOR  (Port 8001)                                        │
│                                                                                        │
│   [connector_worker.py]                                                                │
│          │                                                                             │
│          │ (7. get_connector(request.source_type))                                     │
│          ▼                                                                             │
│   [registry.py] ──> [rss/connector.py: RSSConnector]                                   │
│                            │                                                           │
│                            │ (8. Async HTTP fetch + XML/JSON parser)                   │
│                            ▼                                                           │
│                     [CrawlResult payload]                                              │
│                            │                                                           │
│                            │ (9. XADD to result_stream + XACK connector_stream)        │
└────────────────────────────┼───────────────────────────────────────────────────────────┘
                             │
                             ▼
           ┌──────────────────────────────────────────────────┐
           │  Redis Stream: 'connector:rss:results'           │
           └──────────────────────────────────────────────────┘
                             │
                             ▼ (10. Dual-Loop: _consumer_new_messages + _recover_pending)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                    ARGUS ORCHESTRATOR (continued)                                      │
│                                                                                        │
│   [result_worker.py] ──> [result_service.py: process_crawl_result]                     │
│                                   │                                                    │
│                                   ├──> [MongoDB: rss_items]                            │
│                                   │    (Bulk upsert with item_hash SHA-256 dedup)      │
│                                   │                                                    │
│                                   ├──> [MongoDB: rss_requests]                         │
│                                   │    (status -> 'completed' or 'failed')             │
│                                   │                                                    │
│                                   └──> (11. XACK to result_stream ONLY AFTER PERSIST)  │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### Why This Architecture?
1. **Single-Writer Principle & Durable State:** All permanent database state resides strictly in MongoDB under the Orchestrator's domain. The Connector never connects to MongoDB and holds no database credentials.
2. **Two-Phase Reliable Scheduling:** Scheduled units are indexed by due-timestamp in Redis for fast evaluation. When due, the scheduler first inserts a durable request in MongoDB before advancing the Redis score via a Compare-and-Swap (CAS) Lua script. If a node crashes, the pending MongoDB record ensures the crawl will execute.
3. **Outbound Decoupling & Idempotent Stream Dispatch:** `SchedulerWorker` does not push directly to connector streams. It creates MongoDB requests, allowing the existing `RequestWorker` to claim and dispatch them with Lua-based Redis stream deduplication.
4. **Generic Multi-Connector Extensibility:** Outbound tasks use the generic `ConnectorRequest` schema (`source`, `source_type`, `source_url`, `metadata`). The connector dynamically resolves scrapers via `registry.py` (e.g., `rss`, `nse_announcements`, `nse_circular`).
5. **Bidirectional Fault Tolerance & Pending Recovery:** Both microservices employ Redis Consumer Groups with `XAUTOCLAIM` recovery:
   - `ConnectorWorker` reclaims stale in-flight scraping jobs if a scraper pod terminates.
   - `ResultWorker` reclaims unacknowledged crawl results if an orchestrator instance restarts before database writes finish.
6. **Zero External Bottlenecks:** External HTTP latencies and rate limits stay strictly on the Connector tier. The Orchestrator core processes requests asynchronously in milliseconds.

---

## 2. The Request State Lifecycle

Every crawl task in MongoDB's `rss_requests` collection travels through an auditable, strict state machine:

```text
              ┌──────────────┐
              │   pending    │  <-- Created via scheduler, news query bridge, or manual seed
              └──────┬───────┘
                     │
                     │ (Atomically claimed by request_service.py via find_one_and_update)
                     ▼
              ┌──────────────┐
              │  processing  │  <-- Published to Redis Stream with 'stream_message_id' & 'dispatched_at'
              └──────┬───────┘
                     │
        ┌────────────┴────────────┐
        │ (Connector succeeds)    │ (Connector errors / unreachable URL)
        ▼                         ▼
 ┌──────────────┐          ┌──────────────┐
 │  completed   │          │    failed    │
 └──────────────┘          └──────────────┘
```

### Lifecycle Details & Correlation Keys:
1. **`pending`**:
   - For scheduled runs, created by `scheduled_request_service.py` with deterministic request ID:
     `scheduled:{unit_id}:{scheduled_timestamp:.6f}`
   - For topic query searches, created by `query_dispatch_service.py` with composite `active_query_key`.
   - For manual seeds, created by `scripts/seed_rss_request.py`.
   - Guarantees idempotent retries: if MongoDB raises `DuplicateKeyError` on `request_id`, it is safely treated as an existing retry.
2. **`processing`**:
   - Atomically claimed by `request_worker.py` calling `dispatch_request()`.
   - `status` transitions from `pending` to `processing` with `claimed_at = now`.
   - The request is serialized to `ConnectorRequest` and published to Redis Stream `queue:connector:rss` via an atomic Lua script that stores `argus:dispatch:request:{request_id}:stream_message_id`.
   - MongoDB is updated with `stream_message_id` and `dispatched_at`.
   - **Audit Write Failure & Stale State:** If Redis successfully accepts the message but the subsequent MongoDB audit write fails (e.g. database disconnect, write timeout, or orchestrator crash), the request remains in `processing` with `dispatched_at = None`. Because normal dispatch only polls `pending` requests, this document would remain stranded. The periodic `recover_stale_processing_requests()` background routine resolves this state by reconciling with Redis.
3. **`completed`**:
   - Connector successfully fetches and parses target feed/announcement.
   - Orchestrator's `ResultWorker` receives `CrawlResult` payload.
   - `result_service.process_crawl_result()` bulk-upserts extracted articles into MongoDB `rss_items` using `item_hash` deduplication (`$setOnInsert`).
   - `rss_requests` document is marked `status: "completed"` with `items_count`, `new_items_count`, and `completed_at`.
   - Message is acknowledged in Redis (`xack`).
4. **`failed`**:
   - URL was unreachable, timed out, or returned malformed content.
   - `rss_requests` status transitions to `"failed"`, recording `error_message` and `completed_at`.
   - Message is acknowledged in Redis (`xack`) to prevent infinite poison-pill crash loops.

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
│   ├── main.py               # FastAPI entrypoint with lifespan managing Request, Result & Scheduler workers
│   ├── api/
│   │   └── health.py         # /health and /ready probe endpoints
│   ├── core/
│   │   ├── config.py         # Pydantic Settings loader (including recovery & tick configs)
│   │   └── logging.py        # Centralized logger setup
│   ├── platform/
│   │   ├── mongodb.py        # Async Motor client, collections: news_queries, rss_requests, rss_items, scheduled_units
│   │   └── redis.py          # Async Redis client with decode_responses=True
│   ├── schemas/
│   │   ├── query.py          # NewsQuery model for news_queries collection
│   │   ├── request.py        # Generic ConnectorRequest outbound schema
│   │   └── result.py         # Inbound schema (RSSItem and CrawlResult)
│   ├── services/
│   │   ├── bing_service.py              # Bing search RSS URL builder
│   │   ├── query_dispatch_service.py    # Reads news_queries; creates pending rss_requests
│   │   ├── request_service.py           # Atomic claim, idempotent Redis XADD dispatch, audit logging
│   │   ├── result_service.py            # Article dedup bulk upsert & job completion
│   │   ├── schedule_time_service.py     # Next-run calculation for daily clock times & timezones
│   │   ├── scheduled_request_service.py # Durable idempotent request creation in MongoDB rss_requests
│   │   └── scheduler_service.py         # MongoDB sync, Redis due-time index, CAS advancement & dispatch orchestration
│   ├── test/
│   │   ├── manual_result_worker_recovery.py # Real Redis standalone test for XAUTOCLAIM recovery
│   │   ├── test_redis_dispatch_recovery.py # Regression test: republished entry on stream deletion & dedup reuse
│   │   ├── test_bing.py                  # Verification test for Bing RSS search
│   │   ├── test_request_idempotency.py   # Concurrency race & partial unique index test
│   │   ├── test_request_recovery.py      # Stale request recovery unit & integration tests
│   │   ├── test_request_service.py       # Outbound request service unit tests (claim, publish, audit)
│   │   ├── test_request_worker.py        # RequestWorker recovery loop & health-check tests
│   │   ├── test_result_service.py        # Inbound result service unit tests (dedup, status update)
│   │   ├── test_result_worker.py         # ResultWorker unit test (ACK on success, no ACK on error, recovery)
│   │   ├── test_schedule_time_service.py # Daily next-run calculation unit tests (Asia/Kolkata rollover)
│   │   ├── test_scheduled_request_service.py # Scheduled request creation & retry unit tests
│   │   ├── test_scheduler_dispatch.py    # Mocked dispatch test (Step A/B ordering, CAS script, daily times)
│   │   ├── test_scheduler_service.py     # Manual standalone test for Redis registration & candidate discovery
│   │   ├── test_scheduler_sync.py        # Mocked sync test (MongoDB to Redis registration, disabled/stale prune)
│   │   ├── test_scheduler_sync_due_time.py # Real Redis test: nx=True preserves runtime due scores
│   │   ├── test_scheduler_tick.py        # Candidate sweep & tick dispatch idempotency test
│   │   └── test_scheduler_worker.py      # Background worker lifecycle (sync-before-dispatch & shutdown)
│   └── workers/
│       ├── request_worker.py   # Continuous MongoDB polling dispatcher daemon
│       ├── result_worker.py    # Continuous Redis results consumer daemon with XAUTOCLAIM recovery
│       └── scheduler_worker.py # Autonomous periodic scheduler sync & dispatcher daemon
└── scripts/
    ├── seed_rss_request.py   # CLI tool to seed pending multi-source requests into MongoDB
    └── seed_news_query.py    # Seed query dictionary for targeted Bing news searches
```

### `argus-connector/`
```text
argus-connector/
├── .env                      # Redis URL, stream names, consumer group configuration
├── .env.example              # Template environment variables
├── requirements.txt          # Python dependencies (redis, httpx, feedparser, pydantic-settings, fastapi, uvicorn)
├── test_xautoclaim.py        # Verification script for Redis XAUTOCLAIM stream recovery
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
│   │   ├── rss/
│   │   │   ├── connector.py  # RSSConnector implementing BaseConnector
│   │   │   ├── model.py      # RSSItem & CrawlResult Pydantic models with SHA-256 fingerprinting
│   │   │   └── parser.py     # Async HTTP fetcher + feedparser article extractor
│   │   └── nse/
│   │       ├── __init__.py   # NSE package init
│   │       ├── models.py     # NSEAnnouncement & NSECirculars Pydantic models
│   │       ├── parser.py     # ElementTree XML parser (subject marker & datetime parsing)
│   │       ├── rss.py        # Async HTTP client fetching NSE online announcements feed
│   │       ├── circulars.py  # Async HTTP client fetching NSE circulars JSON API
│   │       └── connector.py  # NSEConnector implementing BaseConnector (announcements & circulars)
│   ├── test/
│   │   ├── test_bing_connector.py # Verification test for RSSConnector with Bing query
│   │   ├── test_bing_parser.py    # Verification test for fetch_and_parse_rss with Bing
│   │   ├── test_nse_circular_api.py # Verification test for live NSE Circulars JSON endpoint
│   │   ├── test_nse_circular_connector.py # Verification test for NSEConnector with source_type="nse_circular"
│   │   ├── test_nse_circular_duplicates.py # Verification test analyzing duplicate circular numbers
│   │   ├── test_nse_circular_worker.py # Verification test for end-to-end NSE circular worker dispatch
│   │   ├── test_nse_connector.py  # Verification test for NSEConnector fetch
│   │   ├── test_nse_parser.py     # Verification test for NSE XML parsing
│   │   ├── test_nse_rss.py        # Verification test for live NSE feed download
│   │   ├── test_nse_worker.py     # Verification test for end-to-end NSE worker flow
│   │   ├── test_redis_group.py    # Redis consumer group creation verification
│   │   ├── test_registry.py       # Connector registry lookup test
│   │   └── test_xpending.py       # Verification test inspecting Redis Stream Pending Entries List (PEL)
│   └── workers/
│       └── connector_worker.py # Continuous consumer loop with XREADGROUP and XAUTOCLAIM stale PEL recovery
```

---

## 4. argus-orchestrator: File-by-File Walkthrough

---

### File 1: `app/core/config.py`
**Purpose:** Single source of truth for all configuration values. Reads from `.env` and provides strongly-typed settings, including stream names, consumer group identifiers, pending-result recovery thresholds, and scheduler tick intervals.

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

    # Redis pending-result recovery (XAUTOCLAIM)
    result_claim_idle_ms: int = 30000                  # Idle threshold before reclaiming stale results (30s)
    result_recovery_interval: int = 10                 # Pause between recovery scans when stream is drained (10s)

    dispatch_tick_seconds: int = 5                     # Cadence for background scheduler worker tick (5s)

    # Request recovery settings
    request_recovery_interval: int = 10                # Cadence for background stale-request recovery (10s)
    request_recovery_timeout_seconds: int = 120        # Staleness threshold for in-flight processing requests (120s)
    request_recovery_lease_seconds: int = 120          # Distributed lease duration for recovery claims (120s)

    @field_validator(
        "request_recovery_interval",
        "request_recovery_timeout_seconds",
        "request_recovery_lease_seconds",
    )
    @classmethod
    def validate_request_recovery_settings(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("Request recovery settings must be greater than zero.")
        return value

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

news_queries = database["news_queries"]       # Target queries dictionary for news search
rss_requests = database["rss_requests"]       # Job queue & status tracking
rss_items = database["rss_items"]             # Parsed and stored articles
scheduled_units = database["scheduled_units"] # Scheduled scraping units (collection handle)

async def check_mongodb():
    """Pings MongoDB admin to verify connection is alive."""
    try:
        await mongo_client.admin.command("ping")
        return True
    except Exception:
        return False
```

> [!NOTE]
> `scheduled_units = database["scheduled_units"]` establishes application access to the MongoDB collection designated for persistent scheduled units. However, as verified in the code, a configuration-sync daemon or automated reconciliation loop between MongoDB and Redis is not yet implemented.

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
**Purpose:** FastAPI application entrypoint. Uses `lifespan` context manager to automatically start background workers (`RequestWorker`, `ResultWorker`, and `SchedulerWorker`) when Uvicorn boots, and cleanly stop them on shutdown.

```python
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.health import router as health_router
from app.core.logging import setup_logging
from app.workers.request_worker import RequestWorker
from app.workers.result_worker import ResultWorker
from app.workers.scheduler_worker import SchedulerWorker

setup_logging()

request_worker = RequestWorker(poll_interval=2.0)
result_worker = ResultWorker(block_ms=2000, batch_size=10)
scheduler_worker = SchedulerWorker()

background_tasks = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start and stop the Orchestrator background workers."""
    print("Argus Orchestrator starting up on Port 8000...")
    background_tasks = [
        asyncio.create_task(request_worker.start(), name="request-worker"),
        asyncio.create_task(result_worker.start(), name="result-worker"),
        asyncio.create_task(scheduler_worker.run(), name="scheduler-worker"),
    ]
    try:
        yield
    finally:
        print("Argus Orchestrator shutting down...")
        request_worker.stop()
        result_worker.stop()
        await scheduler_worker.stop()
        await asyncio.gather(*background_tasks, return_exceptions=True)
        print("Argus Orchestrator shutdown complete.")

app = FastAPI(title="Argus Orchestrator", version="2.0", lifespan=lifespan)
app.include_router(health_router)

@app.get("/")
async def root():
    return {"service": "argus-orchestrator", "status": "running"}
```

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
**Purpose:** Core outbound business logic — atomically claiming a pending task from MongoDB, validating the schema, stringifying metadata for Redis, publishing to Redis Stream with Lua-based deduplication, and updating audit trails.

**Key Mechanics:**
1. **Atomic Claiming:**
   ```python
   claimed_doc = await rss_requests.find_one_and_update(
       {"status": "pending"},
       {"$set": {"status": "processing", "claimed_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc)}},
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
3. **Idempotent Redis Stream Publication & Stream Deletion Recovery via Lua Script:**
   To prevent duplicate messages in the Redis Stream during worker retries while guaranteeing self-healing recovery if a stream entry was trimmed, expired, or deleted via `XDEL`, the service uses an enhanced `_IDEMPOTENT_XADD_SCRIPT` with a per-request deduplication key (`argus:dispatch:request:{request_id}:stream_message_id`):
   ```lua
   local existing_id = redis.call("GET", KEYS[2])

   if existing_id then
       local existing_message = redis.call(
           "XRANGE",
           KEYS[1],
           existing_id,
           existing_id,
           "COUNT",
           1
       )

       if #existing_message > 0 then
           return existing_id
       end
   end

   local message_id = redis.call(
       "XADD",
       KEYS[1],
       "*",
       unpack(ARGV)
   )

   redis.call("SET", KEYS[2], message_id)

   return message_id
   ```
   **Resilience Guarantee:** If Redis already dispatched this request and the message still exists in the stream (`#existing_message > 0`), the script returns `existing_id` without publishing a duplicate. However, if the stream entry was deleted or evicted while the deduplication key still persisted, the script detects `#existing_message == 0`, publishes a replacement message via `XADD`, and updates the deduplication key with the new message ID. This prevents permanent dispatch deadlocks.
4. **Audit Back-Link:**
   Updates MongoDB with `stream_message_id`, `dispatched_at`, and `updated_at`.
5. **Two-Tier Error Handling:**
   - `ValidationError`: Marks request as `"failed"` with the validation message.
   - Network / Redis `Exception`: Rolls status back to `"pending"` with `error_message` recorded, allowing automatic retry on the next tick without task loss.
6. **Existing Stream Message Verification (`_get_existing_stream_message_id`):**
   Reads `argus:dispatch:request:{request_id}:stream_message_id` and verifies with `XRANGE` whether the entry still exists in Redis stream `queue:connector:rss`.
7. **Stale Request Recovery (`recover_stale_processing_requests`):**
   Atomically claims stale processing requests (`claimed_at` older than threshold, `dispatched_at: None`) using `recovery_claimed_at` and a unique `recovery_token`. Reuses verified existing Redis message IDs or publishes idempotently via `_publish_request_idempotently()`, updates MongoDB audit fields, and releases the recovery lease. Catches exceptions and releases the lease, and handles requests missing `request_id` safely.

---

### File 10: `app/workers/request_worker.py`
**Purpose:** Autonomous background dispatcher daemon that calls `dispatch_request()` in a loop and periodically triggers stale request recovery.

**Key Mechanics:**
- **Periodic Stale Request Recovery:** In each processing loop cycle, checks if `now - self._last_recovery_time >= settings.request_recovery_interval` (10s). If elapsed, invokes `recover_stale_processing_requests()`. If recovery raises an exception, logs it with `logger.exception(...)` and allows normal dispatch to proceed uninterrupted.
- **Drain Mode:** When a task is dispatched, sleeps only `10ms` and immediately loops to clear the backlog.
- **Idle Mode:** When no tasks are pending, waits `poll_interval` (2 seconds) using `asyncio.wait_for(self._stop_event.wait(), timeout=...)`.
- **Clean Shutdown:** Catches cancellation and stop signals without dropping tasks.

---

### File 11: `app/schemas/result.py`
**Purpose:** Inbound Pydantic data models defining the payload received from the connector via Redis Stream `connector:rss:results`.

- **`RSSItem`**: A single parsed article stored in `rss_items`:
  - `item_hash`: SHA-256 fingerprint of the normalized URL (`link.strip().lower()`).
  - `request_id`: Originating job ID.
  - `source`: Publisher/source identifier (e.g. `"bing"`, `"nse"`, `"indian_express"`).
  - `source_type`: Connector category (e.g. `"rss"`, `"nse_announcements"`).
  - `title`: Article or announcement title.
  - `link`: Canonical URL.
  - `description`: Optional raw description snippet.
  - `summary`: Cleaned summary text.
  - `author`: Article author.
  - `subject`: Corporate announcement subject or category (e.g. `"OUTCOME OF BOARD MEETING"`).
  - `metadata`: Optional dictionary holding connector-specific metadata attributes (e.g. `circular_number`, `display_number`, `circular_display_date`, `category`, `company`, `department`, `file_size`, `filename`, `file_department`, `file_extension` for `nse_circular`).
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
**Purpose:** Inbound business logic — saving articles to MongoDB with zero duplicates, high-throughput Redis caching, multi-layer deduplication, strict indexing guarantees, and updating request audit status.

**Key Mechanics:**
1. **Two-Tier High-Performance Article Deduplication:**
   Rather than sending thousands of repeated articles into MongoDB `bulk_write()`, the service executes multi-tier deduplication before touching the database:
   - **In-Batch Hash Set Filtering:**
     Discards redundant articles appearing multiple times within the same crawl batch using an in-memory hash set:
     ```python
     unique_items = []
     seen_hashes = set()
     for item in items:
         if item.item_hash and item.item_hash in seen_hashes:
             batch_duplicates += 1
             continue
         if item.item_hash:
             seen_hashes.add(item.item_hash)
         unique_items.append(item)
     ```
   - **Redis Fast-Path `MGET` Lookup (`filter_unseen_items`):**
     Queries Redis cache keys `argus:article:seen:{item_hash}` in a single batched `mget()` call:
     ```python
     ARTICLE_SEEN_PREFIX = "argus:article:seen:"
     ARTICLE_SEEN_TTL_SECONDS = 30 * 24 * 60 * 60  # 30 days

     keys = [f"{ARTICLE_SEEN_PREFIX}{item.item_hash}" for item in hashed_items]
     values = await redis_client.mget(*keys)
     ```
     Articles whose keys return `"1"` are filtered out before persistence, dropping MongoDB CPU and IOPS write overhead by 90%+.
   - **Graceful Redis Degradation:**
     If Redis raises a `ConnectionError` or `TimeoutError`, the service catches the exception with `logger.warning("Redis deduplication lookup failed; falling back to MongoDB.", exc_info=True)` and forwards all items directly to MongoDB. MongoDB remains the authoritative source of truth.
   - **Post-Persistence Pipeline Caching (`mark_items_seen`):**
     After MongoDB `bulk_write()` succeeds, an atomic non-transactional Redis pipeline caches the newly inserted article hashes with a 30-day TTL (`ex=ARTICLE_SEEN_TTL_SECONDS`):
     ```python
     async with redis_client.pipeline(transaction=False) as pipeline:
         for item_hash in hashes:
             pipeline.set(f"{ARTICLE_SEEN_PREFIX}{item_hash}", "1", ex=ARTICLE_SEEN_TTL_SECONDS)
         await pipeline.execute()
     ```
2. **Authoritative MongoDB Zero-Duplicate Bulk Upsert (`$setOnInsert`):**
   ```python
   UpdateOne(
       {"item_hash": item.item_hash},
       {"$setOnInsert": item.model_dump()},
       upsert=True
   )
   ```
   If the hash exists in MongoDB, MongoDB skips it without error. If new, it inserts the document. `$setOnInsert` ensures existing articles preserve their original `created_at` timestamp.
3. **Index Assurance (`ensure_indexes`)**:
   - `rss_items.item_hash`: Unique index `[("item_hash", 1)]` preventing duplicate articles across all crawl runs.
   - `rss_items.request_id` & `rss_items.published_at`: Fast lookup and temporal filtering indexes.
   - `rss_requests.request_id`: Unique sparse index for request lookups.
   - `rss_requests.active_query_key`: Compound partial unique index:
     ```python
     await rss_requests.create_index(
         [("active_query_key", 1)],
         unique=True,
         partialFilterExpression={
             "$and": [
                 {"status": {"$in": ["pending", "processing"]}},
                 {"active_query_key": {"$exists": True}}
             ]
         }
     )
     ```
     Guarantees that at most **one** active (`pending` or `processing`) request can exist for a given query key at any point in time, completely eliminating race-condition double dispatches.
4. **Status Transition & Comprehensive Duplicate Metrics:**
   - On `"success"`: Computes `total_duplicates = stats["duplicates"] + redis_duplicates + batch_duplicates`. Updates `rss_requests` to `"completed"` with `items_count: len(result.items)`, `new_items_count: stats["inserted"]`, and `completed_at`.
   - On `"failed"`: Updates `rss_requests` to `"failed"` with `error_message`.

### File 13: `app/workers/result_worker.py`
**Purpose:** Autonomous inbound background consumer daemon (the "Inbound Ingestor") that continuously reads crawl results from Redis Stream `connector:rss:results` via consumer group `orchestrator-results`, invokes `result_service.process_crawl_result()` to persist articles and request status updates in MongoDB, acknowledges messages via `xack`, and recovers stale in-flight messages via `xautoclaim`.

**Key Mechanics & Reliability Guarantees:**
1. **Pre-Flight Health Checks & Indexes:** On startup, verifies connectivity to both MongoDB and Redis, ensures MongoDB indexes exist via `ensure_indexes()`, and initializes consumer group `orchestrator-results` on stream `connector:rss:results` (`xgroup_create` with `mkstream=True` and `id="0"`).
2. **Dual-Loop Concurrency:** Runs two concurrent `asyncio` task loops via `asyncio.gather()`:
   - **New Message Consumer Loop (`_consumer_new_messages`):** Reads undelivered messages using `xreadgroup` with stream ID `">"` in batches (`batch_size=10`, `block=2000ms`).
   - **Pending Message Recovery Loop (`_recover_pending_messages`):** Periodically inspects the consumer group's Pending Entries List (PEL) using `xautoclaim` with `min_idle_time=settings.result_claim_idle_ms` (30,000 ms / 30s) and consumer name `{consumer_name}-recovery`. Reclaims and retries messages that were abandoned due to worker crash or network timeout. When `start_id == "0-0"` is returned (scan cycle complete), pauses for `settings.result_recovery_interval` (10s).
3. **Payload Normalization:** Handles both raw dictionaries and JSON-string-encoded payloads (`"payload"` or `"items"` string attributes).
4. **Strict Post-Persistence Acknowledgement (`xack`):** Acknowledges messages (`xack`) strictly **after** `process_crawl_result()` succeeds in MongoDB. If MongoDB persistence raises an exception, `xack` is **not** called; the message remains in the Redis PEL and is automatically reclaimed by the recovery loop.
5. **Graceful Lifecycle Management:** Supports OS signals (`SIGINT`, `SIGTERM`) and clean cancellation via `self._stop_event`.

```python
import asyncio
import json
import logging
import signal
from typing import Any, Dict

import redis.exceptions  # Correct import; redis.exception was a previous typo causing AttributeError

from app.core.config import settings
from app.core.logging import setup_logging
from app.platform.mongodb import check_mongodb
from app.platform.redis import check_redis, redis_client
from app.services.result_service import ensure_indexes, process_crawl_result

logger = logging.getLogger(__name__)


class ResultWorker:
    """
    Continuous background consumer that reads incoming crawl results from Redis
    and invokes result_service to persist articles and update request statuses.
    """
    def __init__(
        self,
        block_ms: int = 2000,
        batch_size: int = 10,
        claim_idle_ms: int | None = None,
        recovery_interval: int | None = None,
    ):
        self.stream_name = settings.result_stream
        self.group_name = settings.consumer_group
        self.consumer_name = settings.consumer_name
        self.recovery_consumer_name = f"{self.consumer_name}-recovery"
        self.block_ms = block_ms
        self.batch_size = batch_size

        self.claim_idle_ms = (
            settings.result_claim_idle_ms
            if claim_idle_ms is None
            else claim_idle_ms
        )

        self.recovery_interval = (
            settings.result_recovery_interval
            if recovery_interval is None
            else recovery_interval
        )
        self._is_running = False
        self._stop_event = asyncio.Event()

    async def _setup_consumer_group(self):
        try:
            await redis_client.xgroup_create(
                name=self.stream_name,
                groupname=self.group_name,
                id="0",
                mkstream=True
            )
            logger.info(
                f"Created consumer group '{self.group_name}' on stream '{self.stream_name}'."
            )
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug(
                    f"Consumer group '{self.group_name}' already exists."
                )
            else:
                logger.exception(f"Failed to create consumer group: {exc}")
                raise

    def _parse_message_data(
        self, raw_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        parsed = dict(raw_data)
        if "payload" in parsed and isinstance(parsed["payload"], str):
            try:
                payload = json.loads(parsed["payload"])
                if isinstance(payload, dict):
                    return payload
                raise ValueError("Redis payload must be decoded to a JSON object.")
            except json.JSONDecodeError:
                logger.exception("Invalid JSON in Redis Payload")
                raise

        if "items" in parsed and isinstance(parsed["items"], str):
            try:
                parsed["items"] = json.loads(parsed["items"])
            except json.JSONDecodeError:
                logger.exception("Invalid JSON in Redis items field.")
                raise

        return parsed

    async def _process_message(self, message_id, raw_fields):
        payload = self._parse_message_data(raw_fields)
        summary = await process_crawl_result(payload)

        logger.info(
            "Process result message %s: request_id=%s status=%s, saved=%s",
            message_id,
            summary.get("request_id"),
            summary.get("status"),
            summary.get("items_inserted", 0),
        )

        await redis_client.xack(
            self.stream_name,
            self.group_name,
            message_id,
        )

    async def _consumer_new_messages(self):
        logger.info("New result-message consumer loop started.")

        while self._is_running and not self._stop_event.is_set():
            try:
                entries = await redis_client.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_name: ">"},
                    count=self.batch_size,
                    block=self.block_ms,
                )

                for _, messages in entries or []:
                    for message_id, raw_fields in messages:
                        try:
                            await self._process_message(message_id, raw_fields)
                        except Exception:
                            logger.exception(
                                "Failed to process result message %s.",
                                message_id
                            )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unexpected error in new result-message loop.")
                await asyncio.sleep(5)

        logger.info("New result-message consumer loop exited.")

    async def _recover_pending_messages(self):
        logger.info(
            "Result recovery loop started (idle threshold=%sms).",
            self.claim_idle_ms,
        )

        claim_start_id = "0-0"

        while self._is_running and not self._stop_event.is_set():
            try:
                result = await redis_client.xautoclaim(
                    name=self.stream_name,
                    groupname=self.group_name,
                    consumername=self.recovery_consumer_name,
                    min_idle_time=self.claim_idle_ms,
                    start_id=claim_start_id,
                    count=self.batch_size,
                )

                next_start_id, messages, deleted_ids = result
                claim_start_id = next_start_id

                if deleted_ids:
                    logger.warning(
                        "Redis reported %s deleted pending result IDs.",
                        len(deleted_ids)
                    )

                for message_id, raw_fields in messages:
                    try:
                        logger.info(
                            "Processing reclaimed result message %s.",
                            message_id
                        )
                        await self._process_message(message_id, raw_fields)
                    except Exception:
                        logger.exception(
                            "Failed to process reclaimed result %s.",
                            message_id,
                        )

                if claim_start_id == "0-0":
                    await asyncio.sleep(self.recovery_interval)
            except asyncio.CancelledError:
                raise
            except redis.exceptions.ResponseError as exc:
                logger.exception(
                    "Redis error during result XAUTOCLAIM: %s", exc
                )
                await asyncio.sleep(5)

        logger.info("Result recovery loop exited")

    async def start(self):
        self._is_running = True
        self._stop_event.clear()

        logger.info("Initializing Result Worker...")

        try:
            mongo_ok = await check_mongodb()
            redis_ok = await check_redis()

            if not mongo_ok or not redis_ok:
                logger.error(
                    "Database or Redis is not healthy. Result Worker cannot start."
                )
                return

            await ensure_indexes()
            await self._setup_consumer_group()

            logger.info(
                "Result Worker listening on '%s' [group=%s consumer=%s recovery_consumer=%s].",
                self.stream_name,
                self.group_name,
                self.consumer_name,
                self.recovery_consumer_name,
            )

            await asyncio.gather(
                self._consumer_new_messages(),
                self._recover_pending_messages(),
            )
        finally:
            self._is_running = False
            logger.info("Result Worker loop has exited")

    def stop(self):
        logger.info("Stopping Result Worker gracefully...")
        self._is_running = False
        self._stop_event.set()


async def run_worker():
    setup_logging()
    worker = ResultWorker(block_ms=2000, batch_size=10)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.stop)
        except (NotImplementedError, AttributeError):
            pass

    try:
        await worker.start()
    except (KeyboardInterrupt, asyncio.CancelledError):
        worker.stop()
        logger.info("Result Worker terminated.")


if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        print("\nResult Worker terminated by user.")
```

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

### File 18: `app/services/query_dispatch_service.py`
**Purpose:** Closes the query-to-request automation loop. Reads all enabled search queries from the `news_queries` collection, converts each query into a Bing RSS URL via `build_bing_rss_url()`, and inserts a ready-to-crawl `pending` document into `rss_requests` — without creating duplicates.

#### Why It Exists
Before this service, pending crawl tasks were created exclusively by manually running `scripts/add_rss_request.py` with a hardcoded URL. That meant a human had to be involved every time a new article batch was needed. `query_dispatch_service.py` automates this: it bridges the static query dictionary (seeded by `seed_news_query.py`) to the live crawl queue (`rss_requests`), enabling fully autonomous periodic re-crawling.

#### What It Reads
Queries the `news_queries` MongoDB collection for documents where `enabled: True`. Each document provides:
- `query` — search phrase (e.g. `"business news"`)
- `source` — platform identifier (e.g. `"bing"`)
- `source_type` — connector type (e.g. `"rss"`)
- `topic` — thematic label carried into request metadata
- `priority` — scheduling priority (`HIGH`, `NORMAL`)

#### How It Creates `rss_requests`
For each enabled query, the service:
1. Checks if an **active request** already exists (`status: pending | processing`) for the same `source` + `metadata.query` pair.
2. If one exists, it **skips** the query (preventing double-dispatch).
3. If none exists, it:
   - Builds the target URL: `build_bing_rss_url(query)` → `https://www.bing.com/search?q=...&format=rss`
   - Inserts a fully-formed request document into `rss_requests` with:
     - `status: "pending"`, `request_id`, `source`, `source_type`, `url`, `metadata`, audit timestamps

#### Duplicate Prevention Strategy
The duplicate check uses normalized keys and dual-layer protection:
1. **Application-Level Check:** Computes `active_query_key = f"{source}:{source_type}:{normalized_query}"` and queries `find_one({"active_query_key": active_query_key, "status": {"$in": ["pending", "processing"]}})`. If found, skips creation.
2. **Database-Level Constraint:** If two concurrent dispatchers execute simultaneously, MongoDB's partial unique index on `active_query_key` raises a `DuplicateKeyError`. `query_dispatch_service.py` catches this exception and gracefully logs it as a skipped duplicate:
```python
try:
    await rss_requests.insert_one(request_doc)
    created_count += 1
except DuplicateKeyError:
    skipped_count += 1
    logger.info("Skipping query '%s'. An active request already exists", query)
```
This guarantees that no duplicate crawl jobs are queued regardless of worker concurrency or execution overlap.

#### Architecture Position
```text
[MongoDB: news_queries]                   (seeded by seed_news_query.py)
       │
       │  (1. Find all enabled=True queries)
       ▼
[query_dispatch_service.py]
       │  (2. Duplicate check: any pending|processing for this query?)
       │  (3. build_bing_rss_url(query) → RSS endpoint)
       │  (4. insert_one → rss_requests, status: "pending")
       ▼
[MongoDB: rss_requests]                   (picked up by request_worker.py)
       │
       │  (5. Atomic claim: status → "processing")
       ▼
[request_service.py] → Redis Stream: 'queue:connector:rss'
```

#### How It Connects to `request_worker.py`
`query_dispatch_service.py` and `request_worker.py` are intentionally **decoupled**. The dispatch service only writes to `rss_requests`. The `RequestWorker` daemon independently polls `rss_requests` for `pending` documents every `poll_interval` seconds. This separation means:
- The dispatch service can run on demand, on a schedule, or from an API endpoint.
- The worker never needs to know *where* the pending task came from — it just claims and dispatches whatever is `pending`.

#### How to Run
```bash
# Standalone — from argus-orchestrator directory:
python -m app.services.query_dispatch_service

# Or call programmatically from any async context:
from app.services.query_dispatch_service import dispatch_news_queries
result = await dispatch_news_queries()
# returns: {"created": N, "skipped": M}
```

---

### File 19: `app/test/test_request_idempotency.py`
**Purpose:** Concurrency verification test demonstrating that duplicate active requests with identical `active_query_key` cannot be created concurrently.

**Key Mechanics:**
- Simulates two workers (`worker-A` and `worker-B`) racing via `asyncio.gather()` to insert a request for the exact same `active_query_key`.
- Proves that exactly one worker succeeds (`CREATED`) and the other catches `DuplicateKeyError` (`SKIPPED`).
- Validates that MongoDB's partial unique index strictly caps active requests for the key at `1`.

---

### File 20: `app/services/schedule_time_service.py`
**Purpose:** Pure schedule-time calculation engine for daily clock schedules. Computes the next scheduled execution timestamp strictly after `now`, handling local timezone conversions, clock-time parsing, and day-boundary rollover.

**Key Mechanics & Invariants:**
1. **Clock Time Validation:** Accepts clock strings in 24-hour ISO format (e.g. `["10:00", "19:00"]`). Rejects empty lists, duplicates, and clock strings with embedded offsets (must be local naive times).
2. **Timezone Conversion:** Converts UTC `now` into target timezone (default `"Asia/Kolkata"` / IST) using Python's `zoneinfo.ZoneInfo`.
3. **Same-Day Candidate Search:** Sorts run times chronologically and tests candidates against local `now`. Returns the earliest candidate strictly greater than `local_now`.
4. **Day Rollover:** If all daily run times for today have elapsed (e.g., current time is 19:05 IST), rolls over to tomorrow (`local_now.date() + timedelta(days=1)`) and selects the earliest daily run time (10:00 IST).
5. **UTC Return:** Converts the localized candidate datetime back to UTC for consistent storage and comparisons.

```python
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


def calculate_next_run(
    now: datetime,
    run_times: list[str],
    timezone_name: str = "Asia/Kolkata",
) -> datetime:
    """
    Calculate the next scheduled clock time strictly after `now`.

    Example:
        09:45 IST -> 10:00 IST
        10:05 IST -> 19:00 IST
        19:05 IST -> next day at 10:00 IST
    """
    if not run_times:
        raise ValueError("run_times must not be empty")

    try:
        tz = ZoneInfo(timezone_name)
    except (KeyError, ValueError) as exc:
        raise ValueError(
            f"Invalid timezone: {timezone_name}"
        ) from exc

    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    local_now = now.astimezone(tz)
    parsed_times = []

    for value in run_times:
        try:
            parsed = time.fromisoformat(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid run time: {value}"
            ) from exc

        if parsed.tzinfo is not None:
            raise ValueError(
                "run_times must use local clock times without a timezone"
            )

        parsed_times.append(parsed)

    if len(set(parsed_times)) != len(parsed_times):
        raise ValueError("run_times must not contain duplicates")

    parsed_times.sort()

    for run_time in parsed_times:
        candidate = datetime.combine(
            local_now.date(),
            run_time,
            tzinfo=tz,
        )

        if candidate > local_now:
            return candidate.astimezone(timezone.utc)

    next_day = local_now.date() + timedelta(days=1)

    candidate = datetime.combine(
        next_day,
        parsed_times[0],
        tzinfo=tz,
    )

    return candidate.astimezone(timezone.utc)
```

---

### File 21: `app/services/scheduled_request_service.py`
**Purpose:** Durable MongoDB request persistence service for scheduled units. Creates idempotent `pending` crawl requests in MongoDB `rss_requests` that are later claimed and dispatched by `RequestWorker`.

**Key Mechanics & Design Decisions:**
1. **Deterministic Request ID:** Formats the request ID as `scheduled:{unit_id}:{scheduled_timestamp:.6f}`. Because the timestamp score is fixed to 6 decimal places, any retry or redundant dispatch produces the exact same `request_id`.
2. **MongoDB As Durable Ledger:** Writes full request metadata (`source="bing"`, `source_type="rss"`, `status="pending"`, `url=build_bing_rss_url(term)`, `priority`, `unit_id`, `org_id`, `search_options`, `scheduled_for`).
3. **Idempotency via `DuplicateKeyError`:** If a concurrent dispatch or retry occurs, MongoDB's unique index on `request_id` raises a `DuplicateKeyError`. The service queries `find_one({"request_id": request_id})` to verify the ID collision and returns `{"request_id": request_id, "created": False}`, preventing duplicates while permitting safe retry recovery.
4. **Decoupled Architecture:** Scheduled requests enter the exact same durable pipeline as ad-hoc and seeded query requests. The scheduler does not push directly to connector streams; it relies on `RequestWorker` to claim and dispatch.

```python
import logging
from datetime import datetime, timezone
from typing import Any

from pymongo.errors import DuplicateKeyError

from app.platform.mongodb import rss_requests
from app.services.bing_service import build_bing_rss_url

logger = logging.getLogger(__name__)


async def create_scheduled_rss_request(
    unit_id: str,
    term: str,
    scheduled_for: float,
    priority: str,
    org_id: str = "",
    search_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Create an idempotent RSS request for one scheduled occurrence.

    The request is stored in MongoDB. The existing RequestWorker
    will later publish it to the connector's Redis stream.
    """
    if not unit_id.strip():
        raise ValueError("unit_id must not be empty")

    if not term.strip():
        raise ValueError("term must not be empty")

    if not priority.strip():
        raise ValueError("priority must not be empty")

    scheduled_timestamp = float(scheduled_for)
    request_id = f"scheduled:{unit_id}:{scheduled_timestamp:.6f}"

    now = datetime.now(timezone.utc)
    rss_url = build_bing_rss_url(term)

    request_doc = {
        "request_id": request_id,
        "source": "bing",
        "source_type": "rss",
        "url": rss_url,
        "metadata": {
            "query": term,
            "topic": None,
            "unit_id": unit_id,
            "org_id": org_id,
            "search_options": search_options or {},
            "scheduled_for": scheduled_timestamp,
        },
        "status": "pending",
        "priority": priority.upper(),
        "created_at": now,
        "claimed_at": None,
        "dispatched_at": None,
        "completed_at": None,
        "updated_at": now,
        "stream_message_id": None,
        "items_count": 0,
        "new_items_count": 0,
        "error_message": None,
    }

    try:
        await rss_requests.insert_one(request_doc)

        logger.info(
            "Created scheduled request %s for unit %s",
            request_id,
            unit_id,
        )

        return {
            "request_id": request_id,
            "created": True,
        }

    except DuplicateKeyError:
        existing = await rss_requests.find_one(
            {"request_id": request_id},
            {"_id": 1, "request_id": 1},
        )

        if existing is None:
            raise

        logger.info(
            "Scheduled request %s already exists; treating as a retry",
            request_id,
        )

        return {
            "request_id": request_id,
            "created": False,
        }
```

---

### File 22: `app/services/scheduler_service.py`
**Purpose:** Core scheduling service orchestrating Redis hot-path indices, MongoDB schedule synchronization, due candidate discovery, and atomic two-phase dispatch.

**Key Mechanics & Design Features:**
1. **Redis Hot-Path Keyspace:**
   - `schedule:due:{platform}:{priority}` (Sorted Set): Members are `unit_id`s, scored by next execution timestamp (Unix epoch float).
   - `unit:{unit_id}` (Hash): Holds metadata (`term`, `cadence_seconds`, `schedule_type`, `run_times`, `timezone_name`, `enabled`, etc.).
   - `schedule:active_keys` (Set): Holds `{platform}:{priority}` pairs for sweep discovery.
   - `schedule:managed_unit_ids` (Set): Tracks unit IDs synchronized from MongoDB.
2. **MongoDB Schedule Synchronization (`sync_scheduled_units_from_mongodb`):**
   - Reads active schedules from MongoDB `scheduled_units`.
   - Hydrates Redis with `preserve_due_time=True` (`pipe.zadd(..., nx=True)`), ensuring that existing runtime due scores are **never** overwritten by stale database timestamps.
   - Reconciles deleted or disabled units via set difference (`(managed_ids - seen_ids) | disabled_ids`) and purges them from Redis ZSETs and Hashes.
3. **Compare-And-Swap (CAS) Lua Script (`_ADVANCE_DUE_TIME_SCRIPT`):**
   - Verifies `ZSCORE KEYS[1], unit_id == expected_due_time`.
   - If score matches, updates score to `next_run_at` via `ZADD` and returns `1`.
   - If score was changed or advanced by a concurrent worker, returns `0` without modifying the newer score.
4. **Two-Phase Dispatch Sequence (`dispatch_due_unit`):**
   - **Step A:** Calls `create_scheduled_rss_request()` to ensure durable MongoDB request exists.
   - **Step B:** Calculates `next_run_at` (interval addition or `calculate_next_run()` for daily times) and executes `_ADVANCE_DUE_TIME_SCRIPT`. If CAS returns 0, logs safely and leaves the newer schedule intact.

```python
import logging
import json
import time
from app.services.schedule_time_service import calculate_next_run
from datetime import datetime, timezone

from app.services.scheduled_request_service import (
    create_scheduled_rss_request,
)

from app.platform.redis import redis_client
from app.platform.mongodb import scheduled_units
from app.core.config import settings

logger = logging.getLogger(__name__)

ACTIVE_KEYS_SET = "schedule:active_keys"
MANAGED_UNITS_SET = "schedule:managed_unit_ids"


def _due_key(platform: str, priority: str) -> str:
    return f"schedule:due:{platform.lower()}:{priority.lower()}"


def _unit_key(unit_id: str) -> str:
    return f"unit:{unit_id}"


def _timestamp(value: datetime | int | float) -> float:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    return float(value)


async def register_scheduled_unit(
    unit_id: str,
    platform: str,
    priority: str,
    next_run_at: datetime | int | float,
    term: str,
    cadence_seconds: int,
    org_id: str = "",
    search_options: dict | None = None,
    enabled: bool = True,
    managed_by_mongodb: bool = False,
    preserve_due_time: bool = False,
    schedule_type: str = "interval",
    timezone_name: str = "Asia/Kolkata",
    run_times: list[str] | None = None,
) -> None:
    if (
        isinstance(cadence_seconds, bool)
        or not isinstance(cadence_seconds, int)
        or cadence_seconds <= 0
    ):
        raise ValueError("cadence_second must be a positive integer.")

    if not isinstance(unit_id, str) or not unit_id.strip():
        raise ValueError("unit_id is required.")

    if not isinstance(platform, str) or not platform.strip():
        raise ValueError("platform is required.")

    if not isinstance(priority, str) or not priority.strip():
        raise ValueError("priority is required.")

    platform = platform.strip().lower()
    priority = priority.strip().lower()
    unit_id = unit_id.strip().lower()

    due_key = _due_key(platform, priority)
    unit_key = _unit_key(unit_id)

    metadata = {
        "term": term,
        "org_id": org_id,
        "search_options": json.dumps(search_options or {}),
        "cadence_seconds": str(cadence_seconds),
        "platform": platform,
        "priority": priority,
        "enabled": str(enabled).lower(),
        "schedule_type": schedule_type,
        "timezone_name": timezone_name,
        "run_times": json.dumps(run_times or []),
    }

    async with redis_client.pipeline(transaction=True) as pipe:
        if preserve_due_time:
            pipe.zadd(due_key, {unit_id: _timestamp(next_run_at)}, nx=True)
        else:
            pipe.zadd(due_key, {unit_id: _timestamp(next_run_at)})

        pipe.hset(unit_key, mapping=metadata)
        pipe.sadd(ACTIVE_KEYS_SET, f"{platform}:{priority}")

        if managed_by_mongodb:
            pipe.sadd(MANAGED_UNITS_SET, unit_id)

        await pipe.execute()


async def sync_scheduled_units_from_mongodb() -> int:
    cursor = scheduled_units.find({})
    documents = await cursor.to_list(length=None)

    synchronized_ids: set[str] = set()
    seen_ids: set[str] = set()
    disabled_ids: set[str] = set()

    synchronized_count = 0

    for document in documents:
        unit_id = document.get("unit_id") or document.get("_id")

        if not isinstance(unit_id, str) or not unit_id.strip():
            logger.warning(
                "Skipping scheduled unit with missing or invalid unit_id."
            )
            continue

        unit_id = unit_id.strip()
        seen_ids.add(unit_id)

        enabled = document.get("enabled")
        if not isinstance(enabled, bool):
            logger.warning(
                "Skipping scheduled unit %s: enabled must be a boolean.",
                unit_id,
            )
            continue

        if not enabled:
            disabled_ids.add(unit_id)
            continue

        try:
            platform = document["platform"]
            priority = document["priority"]
            term = document["term"]
            next_run_at = document["next_run_at"]
            cadence_seconds = document["cadence_seconds"]

            schedule_type = document.get("schedule_type", "interval")
            timezone_name = document.get("timezone_name", "Asia/Kolkata")
            run_times = document.get("run_times", [])

            if not isinstance(platform, str) or not platform.strip():
                raise ValueError("platform must be a non-empty string")

            if not isinstance(priority, str) or not priority.strip():
                raise ValueError("priority must be a non-empty string")

            if not isinstance(term, str) or not term.strip():
                raise ValueError("term must be a non-empty string")

            if isinstance(cadence_seconds, bool) or not isinstance(
                cadence_seconds, int
            ):
                raise ValueError("cadence_seconds must be an integer.")

            if cadence_seconds <= 0:
                raise ValueError("cadence_seconds must be positive")

            if not isinstance(next_run_at, (datetime, int, float)):
                raise ValueError(
                    "next_run_at must be a datetime or Unix timestamp"
                )

            if schedule_type not in {"interval", "daily_times"}:
                raise ValueError("unsupported schedule_type")

            if not isinstance(timezone_name, str) or not timezone_name.strip():
                raise ValueError("invalid timezone_name")

            if not isinstance(run_times, list) or not all(
                isinstance(value, str) for value in run_times
            ):
                raise ValueError("run_times must be a list of strings")

            if schedule_type == "daily_times" and not run_times:
                raise ValueError("daily_times requires run_times")

            await register_scheduled_unit(
                unit_id=unit_id,
                platform=platform,
                priority=priority,
                next_run_at=next_run_at,
                term=term.strip(),
                cadence_seconds=cadence_seconds,
                org_id=str(document.get("org_id", "")),
                search_options=document.get("search_options") or {},
                enabled=True,
                managed_by_mongodb=True,
                preserve_due_time=True,
                schedule_type=schedule_type,
                timezone_name=timezone_name,
                run_times=run_times,
            )

            synchronized_ids.add(unit_id)
            synchronized_count += 1

        except (KeyError, TypeError, ValueError) as exc:
            logger.warning(
                "Skipping invalid scheduled unit %s: %s",
                unit_id,
                exc,
            )

    existing_managed_ids = await redis_client.smembers(MANAGED_UNITS_SET)
    stale_ids = (existing_managed_ids - seen_ids) | disabled_ids

    for stale_id in stale_ids:
        unit_key = _unit_key(stale_id)
        metadata = await redis_client.hgetall(unit_key)
        platform = metadata.get("platform")
        priority = metadata.get("priority")

        async with redis_client.pipeline(transaction=True) as pipe:
            if platform and priority:
                due_key = _due_key(platform, priority)
                pipe.zrem(due_key, stale_id)

            pipe.delete(unit_key)
            pipe.srem(MANAGED_UNITS_SET, stale_id)
            await pipe.execute()

        logger.info("Removed stale Mongo-managed schedule: %s", stale_id)

    logger.info(
        "MongoDB schedule synchronization complete: %s unit(s) synchronized.",
        synchronized_count,
    )

    return synchronized_count


_ADVANCE_DUE_TIME_SCRIPT = """
local unit_id = ARGV[1]
local expected_due_time = tonumber(ARGV[2])
local next_run_at = tonumber(ARGV[3])

local current_due_time = redis.call("ZSCORE", KEYS[1], unit_id)

if not current_due_time then
    return 0
end

if tonumber(current_due_time) ~= expected_due_time then
    return 0
end

redis.call("ZADD", KEYS[1], next_run_at, unit_id)

return 1
"""


async def dispatch_due_unit(
    unit_id: str,
    platform: str,
    priority: str,
    now: float | None = None,
) -> dict[str, str] | None:
    current_time = time.time() if now is None else float(now)

    platform = platform.strip().lower()
    priority = priority.strip().lower()

    due_key = _due_key(platform, priority)
    unit_key = _unit_key(unit_id)

    due_score = await redis_client.zscore(due_key, unit_id)
    if due_score is None:
        return None

    scheduled_for = float(due_score)
    if scheduled_for > current_time:
        return None

    metadata = await redis_client.hgetall(unit_key)
    if not metadata:
        logger.warning("Missing metadata for scheduled unit %s", unit_id)
        return None

    if metadata.get("enabled") == "false":
        await redis_client.zrem(due_key, unit_id)
        return None

    term = metadata.get("term")
    stored_platform = metadata.get("platform")
    stored_priority = metadata.get("priority")
    cadence_value = metadata.get("cadence_seconds")

    if not term or not stored_platform or not stored_priority:
        raise ValueError(f"Invalid metadata for scheduled unit {unit_id}")

    if stored_platform != platform or stored_priority != priority:
        logger.warning(
            "Schedule metadata does not match its Redis index: %s",
            unit_id,
        )
        return None

    try:
        cadence_seconds = int(cadence_value)
        search_options = json.loads(metadata.get("search_options") or "{}")
        schedule_type = metadata.get("schedule_type", "interval")
        timezone_name = metadata.get("timezone_name", "Asia/Kolkata")
        run_times = json.loads(metadata.get("run_times") or "[]")

    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid metadata for scheduled unit {unit_id}") from exc

    if cadence_seconds <= 0:
        raise ValueError(f"Invalid cadence for scheduled unit {unit_id}")

    if not isinstance(search_options, dict):
        raise ValueError(f"search_options must be an object for unit {unit_id}")

    if not isinstance(run_times, list) or not all(
        isinstance(value, str) for value in run_times
    ):
        raise ValueError(f"run_times must be a list of strings for unit {unit_id}")

    if schedule_type == "daily_times" and not run_times:
        raise ValueError(f"Daily schedule requires run_times for unit {unit_id}")

    # Step A: create the durable MongoDB request first.
    request = await create_scheduled_rss_request(
        unit_id=unit_id,
        term=term,
        scheduled_for=scheduled_for,
        priority=stored_priority,
        org_id=metadata.get("org_id", ""),
        search_options=search_options,
    )

    # Step B: advance Redis only after request exists
    if schedule_type == "interval":
        next_run_at = current_time + cadence_seconds
    else:
        next_run_at = calculate_next_run(
            now=datetime.fromtimestamp(current_time, tz=timezone.utc),
            run_times=run_times,
            timezone_name=timezone_name,
        ).timestamp()

    advanced = await redis_client.eval(
        _ADVANCE_DUE_TIME_SCRIPT,
        1,
        due_key,
        unit_id,
        scheduled_for,
        next_run_at,
    )

    if int(advanced) != 1:
        logger.info(
            "Schedule %s was already advanced or changed; request %s remains durable",
            unit_id,
            request["request_id"],
        )
        return None

    return {
        "request_id": request["request_id"],
        "next_run_at": str(next_run_at),
        "created": str(request["created"]),
    }


async def get_due_unit_id(
    platform: str,
    priority: str,
    limit: int = 100,
    now: float | None = None,
) -> list[str]:
    if limit <= 0:
        raise ValueError("Limit must be greater than zero")

    due_key = _due_key(platform, priority)
    current_time = time.time() if now is None else now

    return await redis_client.zrangebyscore(
        due_key,
        min="-inf",
        max=current_time,
        start=0,
        num=limit,
    )


async def dispatch_due_units_once(
    limit_per_key: int = 100,
    now: float | None = None,
    platform_priority_keys: set[str] | None = None,
) -> int:
    if limit_per_key <= 0:
        raise ValueError("limit_per_key must be greater than zero")

    current_time = time.time() if now is None else float(now)
    dispatched_count = 0

    if platform_priority_keys is None:
        active_keys = await redis_client.smembers(ACTIVE_KEYS_SET)
    else:
        active_keys = platform_priority_keys

    for active_key in sorted(active_keys):
        try:
            platform, priority = active_key.split(":", 1)
        except ValueError:
            logger.warning("Ignoring invalid scheduler key: %s", active_key)
            continue

        due_unit_ids = await get_due_unit_id(
            platform=platform,
            priority=priority,
            limit=limit_per_key,
            now=current_time,
        )

        for unit_id in due_unit_ids:
            result = await dispatch_due_unit(
                unit_id=unit_id,
                platform=platform,
                priority=priority,
                now=current_time,
            )

            if result is None:
                continue

            dispatched_count += 1
            logger.info(
                "Created scheduled request %s for unit %s; next run at %s",
                result["request_id"],
                unit_id,
                result["next_run_at"],
            )

    return dispatched_count
```

---

### File 23: `app/workers/scheduler_worker.py`
**Purpose:** Autonomous background daemon running periodic schedule ticks. Synchronizes MongoDB definitions before dispatching due units on a configurable timer (`dispatch_tick_seconds`, default 5s).

**Key Mechanics:**
1. **Synchronization Before Dispatch:** `run_once()` executes `sync_scheduled_units_from_mongodb()` *prior* to `dispatch_due_units_once()`. This ensures any new, modified, or disabled schedules in MongoDB are synchronized into Redis before candidate evaluation begins.
2. **Robust Exception Handling:** Ordinary tick failures are caught and logged with full exception tracebacks (`logger.exception("Scheduler tick failed.")`), preventing worker crash.
3. **Graceful Lifecycle Management:** Controlled via `self._stop_event`. Calling `await worker.stop()` unblocks idle waits immediately.

```python
import logging
import asyncio

from app.core.config import settings
from app.services.scheduler_service import (
    dispatch_due_units_once,
    sync_scheduled_units_from_mongodb,
)

logger = logging.getLogger(__name__)


class SchedulerWorker:
    """Runs the scheduler dispatcher periodically."""

    def __init__(self, tick_seconds: int | None = None):
        self.tick_seconds = (
            settings.dispatch_tick_seconds
            if tick_seconds is None
            else tick_seconds
        )

        if self.tick_seconds <= 0:
            raise ValueError("Tick_seconds must be greater than zero.")

        self._stop_event = asyncio.Event()

    async def run_once(self) -> int:
        """Synchronize schedules and execute one dispatcher tick."""
        await sync_scheduled_units_from_mongodb()

        dispatched_count = await dispatch_due_units_once()

        logger.info(
            "Scheduler tick completed. Dispatched %s units.",
            dispatched_count,
        )

        return dispatched_count

    async def run(self) -> None:
        """Run dispatcher tick until shutdown is requested."""
        self._stop_event.clear()

        logger.info(
            "Scheduler worker started. Tick interval: %s seconds.",
            self.tick_seconds,
        )

        while not self._stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                logger.info("Scheduler worker received cancellation signal.")
                raise
            except Exception:
                logger.exception("Scheduler tick failed.")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.tick_seconds,
                )
            except asyncio.TimeoutError:
                pass

        logger.info("Scheduler worker stopped.")

    async def stop(self) -> None:
        """Request a graceful shutdown."""
        self._stop_event.set()
```

---

### File 24: `app/test/test_schedule_time_service.py`
**Purpose:** Pure unit test suite for `calculate_next_run()`. Validates daily schedule calculations against clock times in `Asia/Kolkata` with zero database or Redis dependencies.

**Verifications Covered:**
- `test_before_first_run`: 04:15 UTC (09:45 IST) -> 04:30 UTC (10:00 IST).
- `test_between_daily_runs`: 04:35 UTC (10:05 IST) -> 13:30 UTC (19:00 IST).
- `test_after_last_run`: 14:00 UTC (19:30 IST) -> next day 04:30 UTC (10:00 IST next day).
- `test_exact_run_time_moves_to_next_run`: 04:30 UTC (10:00 IST) -> 13:30 UTC (19:00 IST).
- `test_empty_run_times_rejected`: Rejects empty lists with `ValueError`.

```python
import unittest
from datetime import datetime, timezone

from app.services.schedule_time_service import calculate_next_run


class TestCalculateNextRun(unittest.TestCase):

    RUN_TIMES = ["10:00", "19:00"]

    def test_before_first_run(self):
        now = datetime(2026, 10, 13, 4, 15, tzinfo=timezone.utc)
        result = calculate_next_run(now, self.RUN_TIMES)
        self.assertEqual(result, datetime(2026, 10, 13, 4, 30, tzinfo=timezone.utc))

    def test_between_daily_runs(self):
        now = datetime(2026, 10, 13, 4, 35, tzinfo=timezone.utc)
        result = calculate_next_run(now, self.RUN_TIMES)
        self.assertEqual(result, datetime(2026, 10, 13, 13, 30, tzinfo=timezone.utc))

    def test_after_last_run(self):
        now = datetime(2026, 10, 13, 14, 0, tzinfo=timezone.utc)
        result = calculate_next_run(now, self.RUN_TIMES)
        self.assertEqual(result, datetime(2026, 10, 14, 4, 30, tzinfo=timezone.utc))

    def test_exact_run_time_moves_to_next_run(self):
        now = datetime(2026, 10, 13, 4, 30, tzinfo=timezone.utc)
        result = calculate_next_run(now, self.RUN_TIMES)
        self.assertEqual(result, datetime(2026, 10, 13, 13, 30, tzinfo=timezone.utc))

    def test_empty_run_times_rejected(self):
        with self.assertRaises(ValueError):
            calculate_next_run(datetime.now(timezone.utc), [])


if __name__ == "__main__":
    unittest.main()
```

---

### File 25: `app/test/test_scheduled_request_service.py`
**Purpose:** Mocked integration test suite for `create_scheduled_rss_request()`. Verifies MongoDB document structure, deterministic request ID format, and duplicate retry tolerance.

**Verifications Covered:**
- `test_creates_new_scheduled_request`: Confirms document fields (`status="pending"`, `source="bing"`, `source_type="rss"`, `priority="NORMAL"`, `unit_id="unit-001"`, and deterministic ID `scheduled:unit-001:1791800000.000000`).
- `test_existing_request_is_treated_as_retry`: Verifies that catching `DuplicateKeyError` queries MongoDB and returns `created=False`.
- `test_unmatched_duplicate_error_is_raised`: Ensures unrelated duplicate constraint errors re-raise.

```python
import unittest
from unittest.mock import AsyncMock, patch
from pymongo.errors import DuplicateKeyError
from app.services.scheduled_request_service import create_scheduled_rss_request


class TestScheduledRequestService(unittest.IsolatedAsyncioTestCase):

    async def test_creates_new_scheduled_request(self):
        with (
            patch("app.services.scheduled_request_service.rss_requests") as mock_collection,
            patch("app.services.scheduled_request_service.build_bing_rss_url", return_value="https://example.com/rss"),
        ):
            mock_collection.insert_one = AsyncMock()

            result = await create_scheduled_rss_request(
                unit_id="unit-001",
                term="gold price",
                scheduled_for=1791800000.0,
                priority="normal",
                org_id="org-001",
                search_options={"region": "IN"},
            )

            self.assertTrue(result["created"])
            self.assertEqual(result["request_id"], "scheduled:unit-001:1791800000.000000")

            request_doc = mock_collection.insert_one.await_args.args[0]
            self.assertEqual(request_doc["status"], "pending")
            self.assertEqual(request_doc["source"], "bing")
            self.assertEqual(request_doc["source_type"], "rss")
            self.assertEqual(request_doc["url"], "https://example.com/rss")
            self.assertEqual(request_doc["priority"], "NORMAL")
            self.assertEqual(request_doc["metadata"]["unit_id"], "unit-001")
            self.assertNotIn("active_query_key", request_doc)

    async def test_existing_request_is_treated_as_retry(self):
        with patch("app.services.scheduled_request_service.rss_requests") as mock_collection:
            mock_collection.insert_one = AsyncMock(side_effect=DuplicateKeyError("duplicate"))
            mock_collection.find_one = AsyncMock(return_value={"request_id": "scheduled:unit-001:1791800000.000000"})

            result = await create_scheduled_rss_request(
                unit_id="unit-001",
                term="gold price",
                scheduled_for=1791800000.0,
                priority="NORMAL",
            )

            self.assertFalse(result["created"])
            mock_collection.insert_one.assert_awaited_once()
            mock_collection.find_one.assert_awaited_once()

    async def test_unmatched_duplicate_error_is_raised(self):
        with patch("app.services.scheduled_request_service.rss_requests") as mock_collection:
            mock_collection.insert_one = AsyncMock(side_effect=DuplicateKeyError("duplicate"))
            mock_collection.find_one = AsyncMock(return_value=None)

            with self.assertRaises(DuplicateKeyError):
                await create_scheduled_rss_request(
                    unit_id="unit-001",
                    term="gold price",
                    scheduled_for=1791800000.0,
                    priority="NORMAL",
                )


if __name__ == "__main__":
    unittest.main()
```

---

### File 26: `app/test/test_scheduler_dispatch.py`
**Purpose:** Mocked integration test suite for `dispatch_due_unit()`. Verifies strict request-creation-before-score-advance sequence, Compare-and-Swap protection, daily scheduling next-run calculations, and absence of direct Redis stream publishing.

**Verifications Covered:**
- `test_due_unit_creates_request_with_correct_arguments`: Verifies arguments passed to `create_scheduled_rss_request`.
- `test_due_time_is_advanced_after_request_creation`: Asserts execution order: `["request_created", "advance_due_time"]`.
- `test_changed_due_score_returns_none_and_logs`: Verifies that if CAS Lua returns 0, the function logs and returns `None`.
- `test_future_due_unit_is_not_dispatched`: Future units are skipped without calling request creation.
- `test_missing_metadata_does_not_create_request`: Unregistered units do not trigger request creation.
- `test_scheduler_does_not_publish_to_connector_stream`: Proves `XADD` is never invoked by the scheduler.
- `test_daily_schedule_before_first_run`, `test_daily_schedule_between_run_times`, `test_daily_schedule_after_last_run`: Verifies daily schedule calculation integration with 10:00 AM and 7:00 PM IST clock times.

```python
import logging
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, patch

from app.services import scheduler_service


class TestSchedulerDispatch(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.redis = AsyncMock()
        self.create_request = AsyncMock(
            return_value={
                "request_id": "scheduled:unit-001:1900.000000",
                "created": True,
            }
        )

        self.redis_patch = patch.object(scheduler_service, "redis_client", self.redis)
        self.request_patch = patch.object(scheduler_service, "create_scheduled_rss_request", self.create_request)

        self.redis_patch.start()
        self.request_patch.start()

        self.addCleanup(self.redis_patch.stop)
        self.addCleanup(self.request_patch.stop)

        self.unit_id = "unit-001"
        self.platform = "rss"
        self.priority = "high"
        self.due_score = 1900.0
        self.now = 2000.0
        self.cadence_seconds = 300

        self.due_key = scheduler_service._due_key(self.platform, self.priority)
        self.unit_key = scheduler_service._unit_key(self.unit_id)

        self.metadata = {
            "term": "copper prices",
            "platform": "rss",
            "priority": "high",
            "org_id": "test-org",
            "search_options": '{"language": "en"}',
            "cadence_seconds": str(self.cadence_seconds),
            "enabled": "true",
        }

        self.redis.zscore.return_value = self.due_score
        self.redis.hgetall.return_value = self.metadata
        self.redis.eval.return_value = 1

    async def test_due_unit_creates_request_with_correct_arguments(self):
        result = await scheduler_service.dispatch_due_unit(
            unit_id=self.unit_id,
            platform=self.platform,
            priority=self.priority,
            now=self.now,
        )

        self.create_request.assert_awaited_once_with(
            unit_id="unit-001",
            term="copper prices",
            scheduled_for=1900.0,
            priority="high",
            org_id="test-org",
            search_options={"language": "en"},
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["request_id"], "scheduled:unit-001:1900.000000")
        self.assertEqual(result["next_run_at"], "2300.0")
        self.assertEqual(result["created"], "True")

    async def test_due_time_is_advanced_after_request_creation(self):
        call_order = []

        async def create_request_side_effect(**kwargs):
            call_order.append("request_created")
            return {"request_id": "scheduled:unit-001:1900.000000", "created": True}

        async def eval_side_effect(*args):
            call_order.append("advance_due_time")
            return 1

        self.create_request.side_effect = create_request_side_effect
        self.redis.eval.side_effect = eval_side_effect

        result = await scheduler_service.dispatch_due_unit(
            unit_id=self.unit_id,
            platform=self.platform,
            priority=self.priority,
            now=self.now,
        )

        self.assertIsNotNone(result)
        self.assertEqual(call_order, ["request_created", "advance_due_time"])
        self.redis.eval.assert_awaited_once_with(
            scheduler_service._ADVANCE_DUE_TIME_SCRIPT,
            1,
            self.due_key,
            self.unit_id,
            self.due_score,
            self.now + self.cadence_seconds,
        )

    async def test_changed_due_score_returns_none_and_logs(self):
        self.redis.eval.return_value = 0

        with self.assertLogs(scheduler_service.logger, level=logging.INFO) as captured:
            result = await scheduler_service.dispatch_due_unit(
                unit_id=self.unit_id,
                platform=self.platform,
                priority=self.priority,
                now=self.now,
            )

        self.assertIsNone(result)
        self.create_request.assert_awaited_once()
        self.assertTrue(any("already advanced" in m.lower() for m in captured.output))

    async def test_future_due_unit_is_not_dispatched(self):
        self.redis.zscore.return_value = self.now + 100

        result = await scheduler_service.dispatch_due_unit(
            unit_id=self.unit_id,
            platform=self.platform,
            priority=self.priority,
            now=self.now,
        )

        self.assertIsNone(result)
        self.create_request.assert_not_awaited()
        self.redis.hgetall.assert_not_awaited()
        self.redis.eval.assert_not_awaited()

    async def test_missing_metadata_does_not_create_request(self):
        self.redis.hgetall.return_value = {}

        result = await scheduler_service.dispatch_due_unit(
            unit_id=self.unit_id,
            platform=self.platform,
            priority=self.priority,
            now=self.now,
        )

        self.assertIsNone(result)
        self.create_request.assert_not_awaited()
        self.redis.eval.assert_not_awaited()

    async def test_scheduler_does_not_publish_to_connector_stream(self):
        await scheduler_service.dispatch_due_unit(
            unit_id=self.unit_id,
            platform=self.platform,
            priority=self.priority,
            now=self.now,
        )

        self.redis.xadd.assert_not_awaited()
        self.assertNotIn("XADD", scheduler_service._ADVANCE_DUE_TIME_SCRIPT.upper())

    async def _assert_daily_next_run(self, hour: int, minute: int, expected_day: int, expected_hour: int):
        ist = ZoneInfo("Asia/Kolkata")
        local_now = datetime(2026, 10, 13, hour, minute, tzinfo=ist)
        now_timestamp = local_now.timestamp()

        expected_local = datetime(2026, 10, expected_day, expected_hour, 0, tzinfo=ist)
        expected_timestamp = expected_local.timestamp()

        self.metadata.update({
            "schedule_type": "daily_times",
            "timezone_name": "Asia/Kolkata",
            "run_times": '["10:00", "19:00"]',
        })

        result = await scheduler_service.dispatch_due_unit(
            unit_id=self.unit_id,
            platform=self.platform,
            priority=self.priority,
            now=now_timestamp,
        )

        self.assertIsNotNone(result)
        self.assertAlmostEqual(float(result["next_run_at"]), expected_timestamp)

    async def test_daily_schedule_before_first_run(self):
        await self._assert_daily_next_run(hour=9, minute=45, expected_day=13, expected_hour=10)

    async def test_daily_schedule_between_run_times(self):
        await self._assert_daily_next_run(hour=10, minute=5, expected_day=13, expected_hour=19)

    async def test_daily_schedule_after_last_run(self):
        await self._assert_daily_next_run(hour=19, minute=30, expected_day=14, expected_hour=10)


if __name__ == "__main__":
    unittest.main()
```

---

### File 27: `app/test/test_scheduler_sync.py`
**Purpose:** Mocked integration test suite for `sync_scheduled_units_from_mongodb()`. Verifies schedule registration, validation, error handling, and purge of deleted or disabled units.

**Verifications Covered:**
- `test_enabled_schedule_is_registered`: Verifies that active MongoDB records register in Redis with `preserve_due_time=True`.
- `test_disabled_managed_schedule_is_removed`: Verifies that disabling a schedule removes it from Redis ZSETs and deletes metadata hashes.
- `test_missing_managed_schedule_is_removed`: Purges units deleted from MongoDB.
- `test_invalid_schedules_are_skipped`: Validates graceful skipping on malformed cadence, invalid timezones, or bad timestamps.

```python
import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.services import scheduler_service as scheduler


class TestSchedulerMongoSync(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.mongo_cursor = MagicMock()
        self.mongo_cursor.to_list = AsyncMock(return_value=[])

        self.mongo_collection = MagicMock()
        self.mongo_collection.find.return_value = self.mongo_cursor

        self.redis = MagicMock()
        self.redis.smembers = AsyncMock(return_value=set())
        self.redis.hgetall = AsyncMock(return_value={})

        self.pipeline = MagicMock()
        self.pipeline.execute = AsyncMock()

        self.pipeline_context = MagicMock()
        self.pipeline_context.__aenter__ = AsyncMock(return_value=self.pipeline)
        self.pipeline_context.__aexit__ = AsyncMock(return_value=False)

        self.redis.pipeline.return_value = self.pipeline_context

        self.mongo_patch = patch.object(scheduler, "scheduled_units", self.mongo_collection)
        self.redis_patch = patch.object(scheduler, "redis_client", self.redis)
        self.register_patch = patch.object(scheduler, "register_scheduled_unit", new_callable=AsyncMock)

        self.mongo_patch.start()
        self.redis_patch.start()
        self.mock_register = self.register_patch.start()

        self.addCleanup(self.mongo_patch.stop)
        self.addCleanup(self.redis_patch.stop)
        self.addCleanup(self.register_patch.stop)

    def make_document(self, **overrides):
        document = {
            "unit_id": "test-schedule-001",
            "platform": "rss",
            "priority": "high",
            "term": "copper prices",
            "next_run_at": datetime.now(timezone.utc),
            "cadence_seconds": 300,
            "org_id": "test-org",
            "search_options": {"language": "en"},
            "enabled": True,
        }
        document.update(overrides)
        return document

    async def test_enabled_schedule_is_registered(self):
        document = self.make_document()
        self.mongo_cursor.to_list.return_value = [document]

        count = await scheduler.sync_scheduled_units_from_mongodb()

        self.assertEqual(count, 1)
        self.mock_register.assert_awaited_once_with(
            unit_id="test-schedule-001",
            platform="rss",
            priority="high",
            next_run_at=document["next_run_at"],
            term="copper prices",
            cadence_seconds=300,
            org_id="test-org",
            search_options={"language": "en"},
            enabled=True,
            managed_by_mongodb=True,
            preserve_due_time=True,
            schedule_type="interval",
            timezone_name="Asia/Kolkata",
            run_times=[],
        )

    async def test_disabled_managed_schedule_is_removed(self):
        document = self.make_document(enabled=False)
        self.mongo_cursor.to_list.return_value = [document]
        self.redis.smembers.return_value = {"test-schedule-001"}
        self.redis.hgetall.return_value = {"platform": "rss", "priority": "high"}

        count = await scheduler.sync_scheduled_units_from_mongodb()

        self.assertEqual(count, 0)
        self.pipeline.zrem.assert_called_once_with("schedule:due:rss:high", "test-schedule-001")
        self.pipeline.delete.assert_called_once_with("unit:test-schedule-001")
        self.pipeline.srem.assert_called_once_with(scheduler.MANAGED_UNITS_SET, "test-schedule-001")


if __name__ == "__main__":
    unittest.main()
```

---

### File 28: `app/test/test_scheduler_sync_due_time.py`
**Purpose:** Real Redis integration test confirming that MongoDB-to-Redis synchronization preserves existing runtime due scores in Redis rather than resetting them to stale MongoDB timestamps.

**Verifications Covered:**
- Registers a schedule in real Redis with `original_due_time`.
- Advances score in real Redis to `runtime_due_time`.
- Runs `sync_scheduled_units_from_mongodb()` where MongoDB has `original_due_time`.
- Confirms Redis score remains `runtime_due_time` (guaranteed by `pipe.zadd(..., nx=True)`).
- Cleans up real Redis keys.

```python
import time
import unittest
import uuid
from unittest.mock import patch

from app.platform.redis import redis_client
from app.services import scheduler_service


class FakeCursor:
    def __init__(self, documents):
        self.documents = documents

    async def to_list(self, length=None):
        return self.documents


class FakeScheduledUnitsCollection:
    def __init__(self, documents):
        self.documents = documents

    def find(self, query):
        return FakeCursor(self.documents)


class TestSchedulerSyncDueTime(unittest.IsolatedAsyncioTestCase):

    async def test_sync_preserves_existing_redis_due_time(self):
        unit_id = f"test-preserve-due-{uuid.uuid4().hex}"
        platform = "rss"
        priority = "high"

        due_key = scheduler_service._due_key(platform, priority)
        unit_key = scheduler_service._unit_key(unit_id)

        active_key = f"{platform}:{priority}"
        original_due_time = time.time() + 3600
        runtime_due_time = original_due_time + 600

        try:
            await scheduler_service.register_scheduled_unit(
                unit_id=unit_id,
                platform=platform,
                priority=priority,
                next_run_at=original_due_time,
                term="copper prices",
                cadence_seconds=300,
                org_id="test-org",
                search_options={"language": "en"},
                enabled=True,
                managed_by_mongodb=True,
            )

            await redis_client.zadd(due_key, {unit_id: runtime_due_time})

            score_before_sync = await redis_client.zscore(due_key, unit_id)
            self.assertAlmostEqual(score_before_sync, runtime_due_time, places=3)

            documents = [
                {
                    "unit_id": unit_id,
                    "platform": platform,
                    "priority": priority,
                    "term": "copper prices",
                    "cadence_seconds": 300,
                    "next_run_at": original_due_time,
                    "org_id": "test-org",
                    "search_options": {"language": "en"},
                    "enabled": True,
                }
            ]

            fake_collection = FakeScheduledUnitsCollection(documents)

            with patch.object(scheduler_service, "scheduled_units", fake_collection):
                synchronized_count = await scheduler_service.sync_scheduled_units_from_mongodb()

            score_after_sync = await redis_client.zscore(due_key, unit_id)
            self.assertEqual(synchronized_count, 1)
            self.assertAlmostEqual(score_after_sync, runtime_due_time, places=3)

        finally:
            async with redis_client.pipeline(transaction=True) as pipe:
                pipe.zrem(due_key, unit_id)
                pipe.delete(unit_key)
                pipe.srem(scheduler_service.ACTIVE_KEYS_SET, active_key)
                pipe.srem(scheduler_service.MANAGED_UNITS_SET, unit_id)
                await pipe.execute()


if __name__ == "__main__":
    unittest.main()
```

---

### File 29: `app/test/test_scheduler_tick.py`
**Purpose:** Standalone Redis integration test verifying multi-combination dispatcher sweeps, request creation, future score advancement, idempotency on repeated ticks, and absence of stream publishing.

**Verifications Covered:**
- Registers two distinct scheduled units across different platform/priority combinations.
- Runs `dispatch_due_units_once(now=now)`: confirms exactly 2 units dispatched and requests created.
- Confirms sorted-set scores advanced into the future.
- Executes second tick immediately at identical timestamp: confirms 0 dispatches.
- Confirms connector streams remain empty (no direct `XADD`).

```python
import asyncio
import time
import uuid
from unittest.mock import AsyncMock, patch

from app.platform.redis import redis_client
from app.services import scheduler_service

ACTIVE_KEYS_SET = "schedule:active_keys"
MANAGED_UNITS_SET = "schedule:managed_unit_ids"


async def main():
    suffix = uuid.uuid4().hex

    test_cases = [
        {
            "platform": f"test-a-{suffix}",
            "priority": "high",
            "unit_id": f"test-unit-a-{suffix}",
            "term": "copper prices",
        },
        {
            "platform": f"test-b-{suffix}",
            "priority": "normal",
            "unit_id": f"test-unit-b-{suffix}",
            "term": "gold prices",
        },
    ]

    created_keys = []
    mock_create_request = AsyncMock()

    async def create_request_side_effect(**kwargs):
        return {
            "request_id": f"scheduled:{kwargs['unit_id']}:{kwargs['scheduled_for']:.6f}",
            "created": True,
        }

    mock_create_request.side_effect = create_request_side_effect

    try:
        now = time.time()

        for case in test_cases:
            platform = case["platform"]
            priority = case["priority"]
            unit_id = case["unit_id"]

            await scheduler_service.register_scheduled_unit(
                unit_id=unit_id,
                platform=platform,
                priority=priority,
                next_run_at=now - 1,
                term=case["term"],
                cadence_seconds=300,
            )

            created_keys.append(
                (
                    scheduler_service._due_key(platform, priority),
                    unit_id,
                    scheduler_service._unit_key(unit_id),
                    f"{platform}:{priority}",
                )
            )

        with patch.object(
            scheduler_service,
            "create_scheduled_rss_request",
            mock_create_request,
        ):
            count = await scheduler_service.dispatch_due_units_once(now=now)
            assert count == 2, f"Expected 2 dispatched units, got {count}"

            for case in test_cases:
                due_key = scheduler_service._due_key(case["platform"], case["priority"])
                next_run = await redis_client.zscore(due_key, case["unit_id"])
                assert next_run is not None
                assert next_run > now

            remaining = await scheduler_service.dispatch_due_units_once(now=now)
            assert remaining == 0, f"Expected 0 repeated dispatches, got {remaining}"

        for case in test_cases:
            stream_name = f"queue:connector:{case['platform']}:{case['priority']}"
            messages = await redis_client.xrange(stream_name)
            assert len(messages) == 0

        print("PASS: scheduler dispatch sweep verified successfully")

    finally:
        for due_key, unit_id, unit_key, active_key in created_keys:
            await redis_client.zrem(due_key, unit_id)
            await redis_client.delete(unit_key)
            await redis_client.srem(ACTIVE_KEYS_SET, active_key)
            await redis_client.srem(MANAGED_UNITS_SET, unit_id)

        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
```

---

### File 30: `app/test/test_scheduler_worker.py`
**Purpose:** Unit test suite for `SchedulerWorker` lifecycle. Verifies that synchronization runs strictly before dispatch, periodic execution operates on schedule, and graceful shutdown terminates cleanly.

**Verifications Covered:**
- `test_sync_runs_before_dispatch`: Asserts execution order: `["sync", "dispatch"]`.
- `test_worker_runs_three_ticks_and_stops`: Confirms worker runs exactly 3 ticks and halts when `stop()` is called.

```python
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import app.workers.scheduler_worker as scheduler_module
from app.workers.scheduler_worker import SchedulerWorker


class TestSchedulerWorker(unittest.IsolatedAsyncioTestCase):

    async def test_sync_runs_before_dispatch(self):
        worker = SchedulerWorker(tick_seconds=0.05)
        call_order = []

        async def fake_sync():
            call_order.append("sync")

        async def fake_dispatch():
            call_order.append("dispatch")
            await worker.stop()
            return 0

        with (
            patch.object(
                scheduler_module,
                "sync_scheduled_units_from_mongodb",
                new=AsyncMock(side_effect=fake_sync),
            ),
            patch.object(
                scheduler_module,
                "dispatch_due_units_once",
                new=AsyncMock(side_effect=fake_dispatch),
            ),
        ):
            await asyncio.wait_for(worker.run(), timeout=2)

        self.assertEqual(call_order, ["sync", "dispatch"])

    async def test_worker_runs_three_ticks_and_stops(self):
        worker = SchedulerWorker(tick_seconds=0.05)
        call_count = 0

        async def fake_sync():
            return None

        async def fake_dispatch():
            nonlocal call_count
            call_count += 1
            if call_count == 3:
                await worker.stop()
            return 0

        with (
            patch.object(
                scheduler_module,
                "sync_scheduled_units_from_mongodb",
                new=AsyncMock(side_effect=fake_sync),
            ),
            patch.object(
                scheduler_module,
                "dispatch_due_units_once",
                new=AsyncMock(side_effect=fake_dispatch),
            ),
        ):
            await asyncio.wait_for(worker.run(), timeout=2)

        self.assertEqual(call_count, 3)


if __name__ == "__main__":
    unittest.main()
```

---

### File 31: `app/test/test_scheduler_service.py`
**Purpose:** Real Redis unit test verifying scheduled unit registration, Redis Hash metadata storage, and time-based due lookups with cleanup.

**Verifications Covered:**
- Registers a unit scheduled 60 seconds into the future.
- Confirms hash fields `term` and `cadence_seconds` match in Redis `unit:{unit_id}`.
- Confirms unit is **not** returned when `now = future_time - 1`.
- Confirms unit **is** returned when `now = future_time`.
- Cleans up created test keys.

```python
import asyncio
import time
import uuid

from app.platform.redis import redis_client
from app.services.scheduler_service import (
    register_scheduled_unit,
    get_due_unit_id,
    _due_key,
    _unit_key,
)


async def main():
    unit_id = f"test-unit-{uuid.uuid4().hex}"
    platform = "rss"
    priority = "high"

    due_key = _due_key(platform, priority)
    unit_key = _unit_key(unit_id)

    try:
        future_time = time.time() + 60

        await register_scheduled_unit(
            unit_id=unit_id,
            platform=platform,
            priority=priority,
            next_run_at=future_time,
            term="copper prices",
            cadence_seconds=300,
            org_id="test-org",
            search_options={"language": "en"},
        )

        metadata = await redis_client.hgetall(unit_key)
        assert metadata["term"] == "copper prices"
        assert metadata["cadence_seconds"] == "300"

        early_results = await get_due_unit_id(platform, priority, now=future_time - 1)
        assert unit_id not in early_results

        due_results = await get_due_unit_id(platform, priority, now=future_time)
        assert unit_id in due_results

        print("PASS: scheduled unit registered")
        print("PASS: metadata stored in Redis")
        print("PASS: future unit is not returned early")
        print("PASS: unit is returned when due")

    finally:
        await redis_client.zrem(due_key, unit_id)
        await redis_client.delete(unit_key)
        await redis_client.srem("schedule:active_keys", f"{platform}:{priority}")
        await redis_client.aclose()
        print("Cleaned up test data")


if __name__ == "__main__":
    asyncio.run(main())
```

---

### File 32: `app/test/test_result_worker.py`
**Purpose:** Unit test suite for `ResultWorker`. Verifies acknowledgement after successful processing, absence of acknowledgement on processing failure, and `XAUTOCLAIM` recovery handling.

**Verifications Covered:**
- `test_successful_processing_acknowledges_message`: Asserts `xack` is called after successful `process_crawl_result()`.
- `test_failed_processing_does_not_acknowledge_message`: Asserts `xack` is **not** called when processing raises an error.
- `test_recovery_claims_and_processes_pending_message`: Verifies that `_recover_pending_messages()` uses `xautoclaim` to reclaim and reprocess pending messages.

```python
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app.workers.result_worker import ResultWorker


class TestResultWorker(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.worker = ResultWorker()

    @patch("app.workers.result_worker.redis_client")
    @patch("app.workers.result_worker.process_crawl_result", new_callable=AsyncMock)
    async def test_successful_processing_acknowledges_message(
        self, mock_process_result, mock_redis
    ):
        mock_process_result.return_value = {
            "request_id": "test-request-001",
            "status": "completed",
            "items_inserted": 3,
        }
        mock_redis.xack = AsyncMock()

        message_id = "1234567890-0"
        payload = {"request_id": "test-request-001", "items": []}

        await self.worker._process_message(message_id, payload)

        mock_process_result.assert_awaited_once_with(payload)
        mock_redis.xack.assert_awaited_once_with(
            self.worker.stream_name,
            self.worker.group_name,
            message_id,
        )

    @patch("app.workers.result_worker.redis_client")
    @patch("app.workers.result_worker.process_crawl_result", new_callable=AsyncMock)
    async def test_failed_processing_does_not_acknowledge_message(
        self, mock_process_result, mock_redis
    ):
        mock_process_result.side_effect = RuntimeError("Simulated result-processing failure")
        mock_redis.xack = AsyncMock()

        message_id = "1234567891-0"
        payload = {"request_id": "test-request-002", "items": []}

        with self.assertRaises(RuntimeError):
            await self.worker._process_message(message_id, payload)

        mock_process_result.assert_awaited_once_with(payload)
        mock_redis.xack.assert_not_awaited()

    @patch("app.workers.result_worker.redis_client")
    async def test_recovery_claims_and_processes_pending_message(self, mock_redis):
        message_id = "1234567892-0"
        payload = {"request_id": "test-request-003", "items": []}

        mock_redis.xautoclaim = AsyncMock(
            return_value=("0-0", [(message_id, payload)], [])
        )

        self.worker._is_running = True
        self.worker._stop_event = asyncio.Event()

        with patch.object(self.worker, "_process_message", new_callable=AsyncMock) as mock_process:
            async def stop_after_scan(*args, **kwargs):
                self.worker.stop()
                return ("0-0", [(message_id, payload)], [])

            mock_redis.xautoclaim.side_effect = stop_after_scan

            with patch("app.workers.result_worker.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                await self.worker._recover_pending_messages()

            mock_redis.xautoclaim.assert_awaited_once()
            mock_sleep.assert_awaited_once()
            mock_process.assert_awaited_once_with(message_id, payload)


if __name__ == "__main__":
    unittest.main()
```

---

### File 33: `app/test/manual_result_worker_recovery.py`
**Purpose:** Standalone integration test verifying the `ResultWorker`'s `XAUTOCLAIM` pending message recovery loop against real Redis.

**Scope & Verification:**
- Creates an isolated temporary stream and consumer group (`test:argus:recovery:{uuid}`).
- Delivers a test result to a seed consumer without acknowledging it (leaving it in the PEL).
- Waits until the message exceeds the configured idle threshold (`claim_idle_ms=1000ms`).
- Runs `_recover_pending_messages()` with a mocked `process_crawl_result()`:
  - Verifies `XAUTOCLAIM` successfully reclaims the message.
  - Verifies message processing and `XACK` invocation.
  - Confirms `xpending` count drops to zero.
- Cleans up the temporary test stream.
- *(Scope Note: As `process_crawl_result` is mocked in this test, MongoDB persistence was not verified by this standalone script).*

```python
import asyncio
import uuid
from unittest.mock import AsyncMock, patch

from app.platform.redis import redis_client
from app.workers.result_worker import ResultWorker


async def main():
    test_id = uuid.uuid4().hex
    stream_name = f"test:argus:recovery:{test_id}"
    group_name = f"test-recovery-group:{test_id}"
    seed_consumer = f"test-seed:{test_id}"

    worker = ResultWorker(
        block_ms=100,
        batch_size=10,
        claim_idle_ms=1000,
        recovery_interval=0.01,
    )

    worker.stream_name = stream_name
    worker.group_name = group_name
    worker.consumer_name = f"test-worker:{test_id}"
    worker.recovery_consumer_name = f"test-recovery:{test_id}"

    expected_payload = {
        "request_id": f"recovery-test-{test_id}",
        "items": [],
    }

    try:
        await redis_client.ping()
        print("Redis connection: OK")

        await redis_client.xgroup_create(
            name=stream_name,
            groupname=group_name,
            id="0",
            mkstream=True,
        )

        message_id = await redis_client.xadd(
            stream_name,
            {
                "request_id": expected_payload["request_id"],
                "items": "[]",
            },
        )
        print(f"Test message created: {message_id}")

        delivered = await redis_client.xreadgroup(
            groupname=group_name,
            consumername=seed_consumer,
            streams={stream_name: ">"},
            count=1,
        )

        if not delivered:
            raise AssertionError("The seed consumer received no message.")

        print("Message delivered but deliberately left pending.")
        await asyncio.sleep(1.2)

        async def fake_process_result(payload):
            print(f"Worker processed reclaimed payload: {payload}")
            worker.stop()
            return {
                "request_id": payload["request_id"],
                "status": "completed",
                "items_inserted": 0,
            }

        worker._is_running = True
        worker._stop_event.clear()

        with patch(
            "app.workers.result_worker.process_crawl_result",
            new_callable=AsyncMock,
            side_effect=fake_process_result,
        ) as mock_process:
            await worker._recover_pending_messages()
            mock_process.assert_awaited_once_with(expected_payload)

        pending = await redis_client.xpending(stream_name, group_name)
        if pending["pending"] != 0:
            raise AssertionError(f"Expected zero pending messages, got {pending['pending']}.")

        print("PASS: XAUTOCLAIM recovered the message.")
        print("PASS: Python worker processed the reclaimed message.")
        print("PASS: Message was acknowledged after successful processing.")
        print("PASS: Pending message count is now zero.")

    finally:
        worker.stop()
        await redis_client.delete(stream_name)
        print("Temporary test stream cleaned up.")


if __name__ == "__main__":
    asyncio.run(main())
```

---

### File 34: `app/test/test_result_service.py`
**Purpose:** Comprehensive unit and integration test suite (`TestResultService`) verifying the inbound persistence pipeline, multi-layer deduplication cache, status transitions, and fault tolerance.

**Test Coverage (9 Scenarios):**
1. `test_successful_result_completes_request`: Verifies that a valid crawl result triggers `filter_unseen_items()`, saves unseen items to MongoDB, caches hashes via `mark_items_seen()`, and updates request status to `"completed"` with matched count assertions.
2. `test_failed_result_marks_request_failed`: Verifies that failed crawl results update `rss_requests` to `"failed"` and record the error message.
3. `test_duplicate_result_does_not_insert_duplicate_articles`: Tests that duplicate articles return zero insertions while accurately aggregating duplicate counts across stages.
4. `test_status_update_failure_propagates`: Verifies that MongoDB exceptions during request status updates cleanly propagate so the worker does not acknowledge the Redis message.
5. `test_filter_unseen_items_removes_cached_articles`: Verifies that `filter_unseen_items()` evaluates Redis `mget` responses (`["1", None]`), filters out cached articles, and retains unseen articles (`redis_duplicates = 1`).
6. `test_filter_unseen_items_removes_duplicates_within_batch`: Verifies that multiple copies of the same article within a single batch are deduped in-memory (`batch_duplicates = 1`).
7. `test_filter_unseen_items_falls_back_when_redis_fails`: Simulates a Redis network outage (`ConnectionError: Redis unavailable`) and verifies that the service logs a warning with stack trace and returns all articles untouched so MongoDB uniqueness takes over.
8. `test_mark_items_seen_sets_ttl`: Verifies that `mark_items_seen()` invokes a non-transactional Redis pipeline with `pipeline.set(f"argus:article:seen:{hash}", "1", ex=2592000)` (30-day TTL).
9. `test_unknown_request_is_rejected`: Verifies that a crawl result referencing a non-existent `request_id` raises `ValueError`.

```python
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

from app.schemas.result import CrawlResult, RSSItem
from app.services import result_service


class TestResultService(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.request_id = "test-request-001"
        self.item = RSSItem(
            request_id=self.request_id,
            source="bing",
            source_type="rss",
            title="Copper prices rise",
            link="https://example.com/copper-prices",
            summary="Copper prices increased today.",
        )
        self.success_result = CrawlResult(
            request_id=self.request_id,
            status="success",
            item_count=1,
            items=[self.item],
        )

    @patch("app.services.result_service.redis_client")
    async def test_filter_unseen_items_removes_cached_articles(self, mock_redis):
        mock_redis.mget = AsyncMock(return_value=["1", None])
        second_item = RSSItem(
            request_id=self.request_id,
            source="bing",
            source_type="rss",
            title="Gold prices rise",
            link="https://example.com/gold-prices",
            summary="Gold prices increased today.",
        )
        unseen, redis_duplicates, batch_duplicates = (
            await result_service.filter_unseen_items([self.item, second_item])
        )
        self.assertEqual(unseen, [second_item])
        self.assertEqual(redis_duplicates, 1)
        self.assertEqual(batch_duplicates, 0)
        mock_redis.mget.assert_awaited_once()
```

---

### File 35: `app/test/test_request_service.py`
**Purpose:** Mocked integration test suite (`TestDispatchRequest`) verifying outbound task claim, serialization, Lua script execution, and status transitions.

**Test Coverage (4 Scenarios):**
1. `test_scheduled_request_is_published_to_configured_stream`: Verifies that `dispatch_request()` atomically claims a pending document from MongoDB with `status="processing"`, serializes metadata into JSON, invokes `_IDEMPOTENT_XADD_SCRIPT` on `queue:connector:rss`, and writes `stream_message_id` and timestamps back to MongoDB.
2. `test_audit_failure_does_not_reset_request_to_pending`: Verifies that if Redis accepts the message but MongoDB audit write fails (`RuntimeError`), the request status is **not** reset to `pending` (which would cause a duplicate dispatch), preserving exactly-once semantics.
3. `test_no_pending_request_returns_none`: Verifies that when no `pending` documents exist in `rss_requests`, the dispatcher returns `None` and performs no Redis or MongoDB operations.
4. `test_retry_uses_the_same_deduplication_key`: Verifies that retrying `_publish_request_idempotently()` for the same `request_id` targets the exact same deduplication key (`argus:dispatch:request:{request_id}:stream_message_id`) and returns the original message ID.

```python
import unittest
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from app.services import request_service


class TestDispatchRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.request_id = "scheduled:unit-001:1900.000000"
        self.document_id = "mock-mongo-document-id"
        self.scheduled_document = {
            "_id": self.document_id,
            "request_id": self.request_id,
            "source": "bing",
            "source_type": "rss",
            "url": "https://example.com/rss?q=copper",
            "metadata": {"query": "copper prices", "unit_id": "unit-001"},
            "status": "pending",
        }
```

---

### File 36: `app/test/test_redis_dispatch_recovery.py`
**Purpose:** Real Redis regression test (`TestRedisDispatchRecovery`) executed against Redis verifying stream entry deletion recovery and deduplication key reuse.

**Regression Scenario (`test_deleted_stream_entry_is_republished_once`):**
1. **Initial Publication:** Publishes a message to a test stream using `_IDEMPOTENT_XADD_SCRIPT`. Verifies `original_id` is created and exists in the stream via `XRANGE`.
2. **Simulate Stream Deletion / Truncation:** Deletes the stream entry via `XDEL stream_key original_id`, leaving the deduplication key (`test:argus:dispatch:{suffix} = original_id`) intact in Redis.
3. **Recovery Verification:** Executes the Lua script again for the same task. Proves that the enhanced script detects that the message no longer exists in the stream via `XRANGE`, publishes a replacement message (`replacement_id != original_id`), and updates the deduplication key.
4. **Idempotent Reuse:** Executes the Lua script a third time. Proves that the replacement message is detected in the stream and returned without publishing a duplicate entry (`retry_id == replacement_id`, stream length remains 1).

```python
import unittest
import uuid
import redis.asyncio as redis
from app.services.request_service import _IDEMPOTENT_XADD_SCRIPT


class TestRedisDispatchRecovery(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.redis = redis.Redis(host="localhost", port=6380, decode_responses=True)
        await self.redis.ping()
        suffix = uuid.uuid4().hex
        self.stream_key = f"test:queue:connector:rss:{suffix}"
        self.dedupe_key = f"test:argus:dispatch:{suffix}"
        self.payload = {
            "request_id": f"test-request-{suffix}",
            "source": "test",
            "source_type": "rss",
            "source_url": "https://example.com/test-rss",
        }

    async def asyncTearDown(self):
        await self.redis.delete(self.stream_key, self.dedupe_key)
        await self.redis.aclose()

    async def test_deleted_stream_entry_is_republished_once(self):
        original_id = await self.publish()
        await self.redis.xdel(self.stream_key, original_id)
        replacement_id = await self.publish()
        self.assertNotEqual(replacement_id, original_id)
        retry_id = await self.publish()
        self.assertEqual(retry_id, replacement_id)
```

---

### File 37: `app/test/test_request_recovery.py`
**Purpose:** Comprehensive test suite (`TestRequestRecovery`) verifying `recover_stale_processing_requests()` and `_get_existing_stream_message_id()`:
- Verifies that requests with active recovery leases cannot be claimed by another worker.
- Verifies that existing Redis stream message IDs are verified via `XRANGE` and reused without republishing.
- Verifies fallback to idempotent publishing when message is missing from Redis stream.
- Verifies rejection of non-positive timeout values.
- Verifies that requests missing `request_id` are not dispatched and safely release their recovery claim.

---

### File 38: `app/test/test_request_worker.py`
**Purpose:** Unit test suite for `RequestWorker`:
- Verifies pre-flight health-check behavior.
- Verifies periodic recovery invocation before dispatch.
- Verifies that worker dispatch continues uninterrupted when recovery raises an exception.

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
from app.connectors.nse.connector import NSEConnector

CONNECTORS = {
    "rss": RSSConnector(),
    "nse_announcements": NSEConnector(),
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
    subject: Optional[str] = None # Corporate announcement subject / category
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
**Purpose:** The generic continuous worker daemon that pulls tasks from Redis, safely parses parameters and metadata, resolves the connector via the registry, executes `fetch()`, publishes results, acknowledges with `xack`, and recovers stale unacknowledged messages using `XAUTOCLAIM`.

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
5. **Autonomous Stale Pending Message Recovery (`XAUTOCLAIM`):**
   If a connector worker instance terminates or crashes while executing an external HTTP crawl, the pulled message remains in the Redis consumer group's **Pending Entries List (PEL)**.
   The updated `ConnectorWorker` implements a dedicated background recovery task:
   ```python
   # Runs every recovery_interval (10s)
   next_start_id, messages = await redis_client.xautoclaim(
       name=self.stream_name,
       groupname=self.group_name,
       consumername=self.consumer_name,
       min_idle_time=self.claim_idle_ms,  # 30,000 ms (30s)
       start_id=claim_start_id,
       count=self.batch_size,
   )
   ```
   Any pending message that has been idle for more than `claim_idle_ms` (30 seconds) is transferred to this worker, re-crawled, published to results, and acknowledged via `xack`, preventing orphaned tasks from getting lost forever.

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

> [!NOTE]
> Verification scripts located inside `app/test/` are temporary developer smoke tests used during active connector creation. They can be run individually or eventually migrated to a standard root `tests/` directory with `pytest` mocks.

- **`test_bing_parser.py`**: Validates `fetch_and_parse_rss()` against live Bing News feeds.
- **`test_bing_connector.py`**: Validates dynamic connector dispatch resolving `RSSConnector`.
- **`test_nse_parser.py`**: Validates XML parsing, subject extraction (`|SUBJECT:`), and datetime parsing.
- **`test_nse_rss.py`**: Validates direct HTTP download from `nsearchives.nseindia.com`.
- **`test_nse_connector.py`**: Validates `NSEConnector.fetch()` mapping into standardized `CrawlResult`.
- **`test_nse_worker.py`**: Validates worker dispatch for `source_type="nse_announcements"`.
- **`test_nse_circular_api.py`**: Validates direct HTTP calls to the NSE Circulars REST endpoint (`https://www.nseindia.com/api/circulars`) with date parameters and extracts parsed circular records.
- **`test_nse_circular_duplicates.py`**: Validates circular date parsing and checks for duplicate circular numbers and display numbers across multiple departments.
- **`test_nse_circular_connector.py`**: Validates `NSEConnector.fetch()` mapping circulars into standardized `CrawlResult` and `RSSItem` with full metadata payload.
- **`test_nse_circular_worker.py`**: Verifies end-to-end Redis Stream dispatch (`connector:requests` -> `connector_worker` -> `connector:rss:results`).
- **`test_redis_group.py`**: Verifies Redis consumer group initialization.
- **`test_registry.py`**: Verifies registry lookup for `"rss"`, `"nse_announcements"`, and `"nse_circular"`.
- **`test_xpending.py`**: Inspects Redis Stream Pending Entries List (`XPENDING`) to observe in-flight message state.
- **`test_xautoclaim.py`**: Verifies `XAUTOCLAIM` recovery by seeding unacknowledged pending tasks and asserting automatic worker reclamation.

---

### Connector File 13: `app/connectors/nse/models.py`
**Purpose:** Dedicated Pydantic models representing corporate announcements and official regulatory circulars from the National Stock Exchange of India (NSE).

```python
from datetime import datetime, timezone
from typing import Optional 
from pydantic import BaseModel, Field

class NSEAnnouncement(BaseModel):
    request_id: str
    source: str
    source_type: str
    title: str
    link: Optional[str] = None
    description: Optional[str] = None
    subject: Optional[str] = None
    published_at: Optional[datetime] = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class NSECirculars(BaseModel):
    request_id: str
    source: str
    source_type: str
    circular_date: Optional[datetime] = None
    circular_display_date: Optional[str] = None
    category: Optional[str] = None
    company: Optional[str] = None
    department: Optional[str] = None
    display_number: Optional[str] = None
    circular_number: Optional[str] = None
    subject: Optional[str] = None
    file_link: Optional[str] = None
    file_size: Optional[str] = None
    filename: Optional[str] = None
    file_department: Optional[str] = None
    file_extension: Optional[str] = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
```

---

### Connector File 14: `app/connectors/nse/parser.py`
**Purpose:** XML parser using standard library `xml.etree.ElementTree` with specialized extraction for NSE description metadata and publication dates.

**Key Mechanics:**
1. **Subject Marker Extraction (`parse_subject`)**:
   NSE online announcements pack corporate subjects into the description string following a `|SUBJECT:` delimiter (e.g. `"... |SUBJECT: Financial Results"`). The parser safely splits and extracts this string:
   ```python
   def parse_subject(description: Optional[str]) -> Optional[str]:
       if not description or "|SUBJECT:" not in description:
           return None
       return description.split("|SUBJECT:", 1)[1].strip() or None
   ```
2. **Custom Date Formatting (`parse_published_at`)**:
   Parses NSE's specific timestamp format (`%d-%b-%Y %H:%M:%S`, e.g., `29-Sep-2026 17:45:00`) and converts it to a timezone-aware UTC datetime.
3. **ElementTree Traversal (`parse_nse_rss`)**:
   Iterates through each `<item>` node in the RSS feed and maps elements into clean `NSEAnnouncement` models.

---

### Connector File 15: `app/connectors/nse/rss.py`
**Purpose:** Asynchronous HTTP client configured specifically to retrieve the official NSE online announcements feed.

**Key Mechanics:**
- **Endpoint:** `https://nsearchives.nseindia.com//content/RSS/Online_announcements.xml`
- **Browser-Identical Headers:** Uses a Chrome Windows user-agent and XML accept headers to prevent HTTP 403 / anti-scraping blocks from NSE's archive servers.
- **Timeout & Redirects:** 30.0s timeout with `follow_redirects=True`.
- Returns parsed list of `NSEAnnouncement` objects via `parse_nse_rss()`.

---

### Connector File 16: `app/connectors/nse/connector.py`
**Purpose:** Concrete implementation of `BaseConnector` for NSE Corporate Announcements and NSE Circulars.

**Key Mechanics:**
- Implements `async def fetch(self, request: ConnectorRequest) -> CrawlResult`.
- Dispatches based on `request.source_type`:
  - `"nse_announcements"`: Calls `_fetch_announcements()` -> `fetch_nse_rss()` -> `_announcement_to_rss_item()`.
  - `"nse_circular"`: Calls `_fetch_circulars()` -> `fetch_nse_circulars()` -> `_circular_to_rss_item()`.
- Standardizes both data streams into `RSSItem` objects:
  - Preserves `subject` across both sources.
  - Attaches rich circular metadata (`circular_number`, `display_number`, `circular_display_date`, `category`, `company`, `department`, `file_size`, `filename`, `file_department`, `file_extension`) in `RSSItem.metadata`.
- Returns a standardized `CrawlResult(status="success", item_count=len(items), items=items)`.

```python
class NSEConnector(BaseConnector):

    async def fetch(self, request: ConnectorRequest) -> CrawlResult:
        if request.source_type == "nse_announcements":
            return await self._fetch_announcements(request)
        if request.source_type == "nse_circular":
            return await self._fetch_circulars(request)
        raise ValueError(f"Unsupported NSE source_type: {request.source_type}")

    async def _fetch_announcements(self, request: ConnectorRequest) -> CrawlResult:
        announcements = await fetch_nse_rss(request_id=request.request_id)
        items = [self._announcement_to_rss_item(a) for a in announcements]
        return CrawlResult(request_id=request.request_id, status="success", item_count=len(items), items=items)

    async def _fetch_circulars(self, request: ConnectorRequest) -> CrawlResult:
        from_date = request.metadata.get("from_date")
        to_date = request.metadata.get("to_date")
        if not from_date or not to_date:
            raise ValueError("from_date and to_date are required for NSE circulars")
        circulars = await fetch_nse_circulars(request_id=request.request_id, from_date=from_date, to_date=to_date)
        items = [self._circular_to_rss_item(c) for c in circulars]
        return CrawlResult(request_id=request.request_id, status="success", item_count=len(items), items=items)

    @staticmethod
    def _announcement_to_rss_item(announcement: NSEAnnouncement) -> RSSItem:
        return RSSItem(
            request_id=announcement.request_id,
            source=announcement.source,
            source_type=announcement.source_type,
            title=announcement.title,
            link=announcement.link or "",
            description=announcement.description,
            subject=announcement.subject,
            published_at=announcement.published_at,
        )

    @staticmethod
    def _circular_to_rss_item(circular: NSECirculars) -> RSSItem:
        return RSSItem(
            request_id=circular.request_id,
            source=circular.source,
            source_type=circular.source_type,
            title=circular.subject or circular.display_number or "NSE Circular",
            link=circular.file_link or "",
            description=circular.subject,
            subject=circular.subject,
            published_at=circular.circular_date,
            metadata={
                "circular_number": circular.circular_number,
                "display_number": circular.display_number,
                "circular_display_date": circular.circular_display_date,
                "category": circular.category,
                "company": circular.company,
                "department": circular.department,
                "file_size": circular.file_size,
                "filename": circular.filename,
                "file_department": circular.file_department,
                "file_extension": circular.file_extension,
            },
        )
```

---

### Connector File 17: `app/connectors/nse/circulars.py`
**Purpose:** Asynchronous HTTP client configured specifically to retrieve official NSE circulars via REST/JSON endpoint.

**Key Mechanics:**
- **Endpoint:** `https://www.nseindia.com/api/circulars` with `fromDate` and `toDate` query parameters (`DD-MM-YYYY`).
- **Browser-Identical Headers:** Uses a Chrome Windows user-agent, JSON accept headers, and `Referer: https://www.nseindia.com/` to avoid HTTP 403 blocks from NSE's servers.
- **Date Parsing (`_parse_circular_date`)**: Parses NSE's `%Y%m%d` date format (e.g., `20260930`) into a timezone-aware UTC `datetime`.
- **Field Normalization (`_parse_circular`)**: Extracts and maps raw dictionary keys (`circNumber`, `circDisplayNo`, `circCategory`, `circCompany`, `circDepartment`, `circFileSize`, `circFilelink`, `circFilename`, `fileDept`, `fileExt`, `sub`) into structured `NSECirculars` models with `source_type="nse_circular"`.
- **Timeout & Redirects:** 30.0s timeout with `follow_redirects=True`.

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

### Bug 18: NSE Announcements Feed Subject and Date Extraction
- **Symptom:** NSE XML feed formats publish dates as `%d-%b-%Y %H:%M:%S` rather than standard RFC 822 / ISO 8601, and embeds the announcement subject inside the description prefixed by `|SUBJECT:`. Standard generic RSS parsers failed to extract the subject and threw parsing errors on dates.
- **Fix:** Implemented custom ElementTree extraction in `argus-connector/app/connectors/nse/parser.py` with `parse_subject()` and `parse_published_at()` mapping into `NSEAnnouncement` and propagating `subject` into `RSSItem`.

### Bug 19: Concurrent Query Dispatch Race Condition
- **Symptom:** When running multiple workers or recurring dispatch sweeps, multiple identical `pending` requests could be inserted into `rss_requests` before earlier tasks were claimed or processed.
- **Fix:** In `argus-orchestrator/app/services/result_service.py`, added a partial unique index on `active_query_key` for documents with `status: ["pending", "processing"]`. In `query_dispatch_service.py`, normalized queries and caught `DuplicateKeyError` to guarantee exactly-once dispatch idempotency.

### Bug 20: Missing Trailing Comma & Attribute Mismatches in NSE Circulars Mapper
- **Symptom:** Python parse error `Parse error: Expected ',', found name @[connector.py:L111]` caused by a missing comma after `published_at=circular.published_at` inside `_circular_to_rss_item()`. Additionally, `AttributeError` exceptions occurred when evaluating `circular.title`, `circular.link`, and `circular.published_at` because `NSECirculars` used `subject`, `file_link`, and `circular_date`.
- **Fix:** Added the missing comma, mapped `circular.file_link` -> `link`, `circular.subject` -> `title`/`description`, `circular.circular_date` -> `published_at`, and extracted `circFileSize` from the NSE API into `circular.file_size` and `metadata["file_size"]`.

### Bug 21: Canonical `source_type` Naming Inconsistency for NSE Circulars
- **Symptom:** Inconsistent naming between plural `"nse_circulars"` and singular `"nse_circular"` caused worker tasks and tests to fail with `ValueError: Unsupported NSE source_type: nse_circular`.
- **Fix:** Standardized the canonical connector `source_type` across `app/connectors/registry.py`, `NSEConnector.fetch()`, `app/connectors/nse/circulars.py`, and test suites strictly to singular `"nse_circular"`.

### Bug 22: Redis Dispatch Deadlock on Stream Truncation / Message Eviction
- **Where:** [`app/services/request_service.py`](file:///C:/Users/OmMangeshBangar/PycharmProjects/argus-2.0/argus-orchestrator/app/services/request_service.py) — `_IDEMPOTENT_XADD_SCRIPT`.
- **Symptom:** In production or high-throughput testing, when messages in `queue:connector:rss` were evicted by stream `MAXLEN` or deleted via `XDEL`, retrying `dispatch_request()` immediately returned the old `existing_id` because `redis.call("GET", KEYS[2])` still held the previous message ID. However, because the message was gone from the stream, downstream connector workers never received the job, permanently stranding the request.
- **Fix:** Enhanced `_IDEMPOTENT_XADD_SCRIPT` to verify whether the stream entry still exists using `redis.call("XRANGE", KEYS[1], existing_id, existing_id, "COUNT", 1)`. If `#existing_message == 0`, the script automatically generates a new `XADD` entry and updates the deduplication key with the new message ID. Verified in regression test [`app/test/test_redis_dispatch_recovery.py`](file:///C:/Users/OmMangeshBangar/PycharmProjects/argus-2.0/argus-orchestrator/app/test/test_redis_dispatch_recovery.py).

### Bug 23: `AttributeError` on Redis ResponseError in Result Worker
- **Where:** [`app/workers/result_worker.py`](file:///C:/Users/OmMangeshBangar/PycharmProjects/argus-2.0/argus-orchestrator/app/workers/result_worker.py) — `_recover_pending_messages()`.
- **Symptom:** During pending message recovery, an unhandled exception occurred: `AttributeError: module 'redis' has no attribute 'exception'`.
- **Root Cause:** A typo in the exception catch clause: `except redis.exception.ResponseError` instead of `except redis.exceptions.ResponseError`.
- **Fix:** Corrected the catch clause to `except redis.exceptions.ResponseError as exc:`.

### Bug 24: Unbounded MongoDB Duplicate Write Amplification & Multi-Layer Redis Deduplication Cache
- **Where:** [`app/services/result_service.py`](file:///C:/Users/OmMangeshBangar/PycharmProjects/argus-2.0/argus-orchestrator/app/services/result_service.py).
- **Symptom:** RSS and news crawlers frequently re-scrape unchanged feeds, producing thousands of identical articles. Passing all items into MongoDB `rss_items.bulk_write()` caused high disk IOPS, locking, and CPU usage, even though unique indexes on `item_hash` prevented duplicate insertions.
- **Fix:** Implemented a two-tier high-performance caching layer:
  1. In-batch set filtering to drop duplicate hashes in memory.
  2. Redis cache `argus:article:seen:{hash}` evaluated via batched `mget()` with a 30-day TTL (`ARTICLE_SEEN_TTL_SECONDS = 2592000`).
  3. Non-blocking fallback: If Redis is unavailable, the service catches the exception, logs a warning, and falls back seamlessly to MongoDB uniqueness.
  4. Post-persistence pipeline caching via `mark_items_seen()`.

---

## 8. Current Roadmap & Project Progress

```text
[x] Phase 1:  Environment & Async Database Drivers (Motor & Redis)
[x] Phase 2:  Configuration & Logging Setup
[x] Phase 3:  Outbound Request Schema & Atomic Claim Dispatch
[x] Phase 15: Query Dispatch Service & Concurrent Race Prevention
[x] Phase 17: Reliable Scheduled RSS Request Dispatch & Pending Result Recovery
              - app/services/scheduled_request_service.py (durable MongoDB pending request creation)
              - app/services/scheduler_service.py (two-phase dispatch via Compare-and-Swap Lua script _ADVANCE_DUE_TIME_SCRIPT)
              - app/workers/scheduler_worker.py (sync before dispatch tick, robust exception logging, graceful shutdown)
              - app/workers/result_worker.py (dual-loop XAUTOCLAIM stale message recovery with idle threshold)
              - app/platform/mongodb.py (scheduled_units durable collection)
              - Resolved all prototype defects 1 through 8 (keyspace prefix, ACTIVE_KEYS_SET format, dedented loop return, exception traceback)
[x] Phase 19: Daily Scheduling Support (Clock Times & Timezones)
              - app/services/schedule_time_service.py (calculate_next_run with local clock times e.g. ["10:00", "19:00"] and Asia/Kolkata timezone)
              - app/services/scheduler_service.py (integration with daily clock times and day rollover)
              - app/test/ (test_schedule_time_service, test_scheduler_dispatch daily runs, test_scheduler_sync daily metadata)
[x] Phase 20: Redis Article Deduplication Cache (30-Day TTL)
              - app/services/result_service.py (filter_unseen_items with in-batch set filter + Redis MGET)
              - app/services/result_service.py (mark_items_seen with pipeline caching and 30-day TTL)
              - app/test/test_result_service.py (9 comprehensive test cases verifying dedup, fallback, TTL)
[x] Phase 21: Stream Recovery in Idempotent Lua Dispatch Script & Exception Hardening
              - app/services/request_service.py (_IDEMPOTENT_XADD_SCRIPT with XRANGE stream existence check)
              - app/workers/result_worker.py (fixed redis.exceptions.ResponseError catch typo)
              - app/test/test_redis_dispatch_recovery.py (regression test proving recovery after XDEL)
              - Full 42-test suite passing across all unit and integration layers (Historical: Ran 42 tests in 2.700s ... OK at Phase 21 completion)
──────────────────────────────────────────────────────────────────────────────────────────
[ ] Phase 11: BSE Corporate Announcements Connector
[ ] Phase 12: SEBI Orders & Circulars Connector (HTML scraper + PDF link extractor)
[ ] Phase 13: SAT (Securities Appellate Tribunal) Orders Connector
[ ] Phase 14: Stocktwits Social Feed Connector (REST API + JSON)
[ ] Phase 16: REST API Endpoint (POST /api/feeds) to manage requests via HTTP
[ ] Phase 18: Unified Data Model (single 'market_intelligence_items' collection)
```

### Implementation & Verification Status Checklist

#### Implemented and Unit/Integration Tested:
- **Daily Next-Run Calculation:** Clock time parsing (`["10:00", "19:00"]`), `Asia/Kolkata` timezone conversion, intra-day candidate advancement, and next-day rollover. Verified in `test_schedule_time_service.py`.
- **Interval Scheduling Compatibility:** Preserved seamlessly alongside daily scheduling (`schedule_type="interval"` vs `schedule_type="daily_times"`).
- **Two-Phase Scheduler Dispatch:** First creates a durable MongoDB `pending` request in `rss_requests` with deterministic ID `scheduled:{unit_id}:{scheduled_timestamp:.6f}`, then advances the Redis score via Compare-and-Swap Lua. Eliminates stream bypassing and aligns with existing request pipeline.
- **Compare-And-Swap (CAS) Lua Script (`_ADVANCE_DUE_TIME_SCRIPT`):** Advances Redis sorted set score if and only if the current score matches the expected due score, preventing double-advance race conditions.
- **MongoDB-to-Redis Synchronization Before Dispatch:** Synchronizes definitions on every tick with `preserve_due_time=True` (`pipe.zadd(..., nx=True)`), preventing overwriting of active runtime scores. Purges disabled and deleted schedules via set difference against `schedule:managed_unit_ids`. Verified in `test_scheduler_sync.py` and `test_scheduler_sync_due_time.py`.
- **Scheduler Worker Lifecycle:** Full worker lifecycle, periodic ticking, exception handling with stack traces, and graceful shutdown verified in `test_scheduler_worker.py`.
- **Inbound Result Worker Recovery (`XAUTOCLAIM`):** Dual-loop consumption (`_consumer_new_messages` and `_recover_pending_messages`) with configurable idle threshold (`result_claim_idle_ms=30000`) and recovery interval (`result_recovery_interval=10s`), post-persistence `xack`. Verified in `test_result_worker.py` and `manual_result_worker_recovery.py`.
- **Redis Article Deduplication:** In-batch hash filtering, Redis fast-path `mget` lookup, 30-day TTL pipeline caching, and graceful fallback to MongoDB uniqueness. Verified in `test_result_service.py`.
- **Stream Deletion Self-Healing:** Enhanced `_IDEMPOTENT_XADD_SCRIPT` validates stream presence via `XRANGE` and automatically republishes replacement messages if stream entries were deleted or evicted. Verified in `test_redis_dispatch_recovery.py`.
- **Result Worker Hardening:** Fixed exception typo to `redis.exceptions.ResponseError`, guaranteeing unbroken dual-loop `XAUTOCLAIM` recovery.
- **Test Suite Health:** All 54 tests passing in `argus-orchestrator`: `Ran 54 tests in 2.044s ... OK` (historical baseline: 42 tests passing in 2.700s prior to stale request recovery).

#### Still Requiring Live Verification:
- **Initial Registration of New Daily Schedules:** Creating a newly seeded daily schedule document in MongoDB and verifying its very first calculated due time in Redis.
- **Complete Real End-to-End Scheduled Run:** Executing the entire pipeline concurrently in a live environment:
  MongoDB `scheduled_units` -> Redis Hot-Path ZSET -> Scheduler creates MongoDB `rss_requests` -> `RequestWorker` dispatches to Redis Stream `queue:connector:rss` -> `ConnectorWorker` crawls Bing RSS -> Connector publishes to `connector:rss:results` -> `ResultWorker` ingests and persists to MongoDB `rss_items`.
- **Failure & Retry Behavior Through Live Pipeline:** Live verification of network partitions, worker crashes during crawl, retry handling with `DuplicateKeyError`, and `XAUTOCLAIM` reclamation of actual unacknowledged batches.
- **Generalized Multi-Source Scheduled Dispatch:** Currently, `create_scheduled_rss_request()` hardcodes Bing RSS URL building (`build_bing_rss_url(term)`). Future scheduled dispatches for NSE announcements or circulars will require platform-aware request building.

---

## 9. Implementation Guide: Redis Scheduler, Daily Scheduling & Ingest Recovery

This section documents the production-grade scheduling, synchronization, and recovery subsystems in [`argus-orchestrator`](file:///C:/Users/OmMangeshBangar/PycharmProjects/argus-2.0/argus-orchestrator), detailing their architectural boundaries, data structures, atomic execution semantics, test verification, defect resolutions, and operational guidelines.

---

### 9.1 System Architecture & End-to-End Pipeline

Argus 2.0 strictly enforces the **Single-Writer Principle**: `argus-orchestrator` is the sole authority with access to MongoDB. `argus-connector` is completely stateless, has no database credentials, and communicates solely via Redis Streams.

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   ARGUS ORCHESTRATOR                                   │
│                                                                                        │
│   ┌───────────────────────────┐  sync_from_mongo()   ┌─────────────────────────────┐   │
│   │   Operational Store       │ ────────────────────> │      Hot-Path Scheduler     │   │
│   │   [MongoDB: argus]        │  (preserves runtime   │      [Redis: Port 6379]     │   │
│   │   - scheduled_units (Doc) │   scores via nx=True) │  - schedule:active_keys     │   │
│   │   - news_queries          │                       │  - schedule:managed_unit_ids│   │
│   │   - rss_requests          │ <───────────────────  │  - schedule:due:{p}:{pri}   │   │
│   │   - rss_items             │  1. create_scheduled_ │  - unit:{unit_id} (HASH)    │   │
│   │                           │     rss_request()     │                             │   │
│   │                           │                       └──────────────┬──────────────┘   │
│   │                           │  2. _ADVANCE_DUE_TIME_SCRIPT (CAS)   │                  │
│   │                           │     advances score in Redis ZSET     │                  │
│   │                           │                                      │                  │
│   │                           │  ┌───────────────────────────────────┴──────────────┐   │
│   │                           │  │ SchedulerWorker: run_once() [sync -> dispatch]   │   │
│   │                           │  │ - runs every dispatch_tick_seconds (default 5s)  │   │
│   │                           │  └──────────────────────────────────────────────────┘   │
│   │                           │                                                         │
│   │   [rss_requests: pending] │                                                         │
│   │             │             │                                                         │
│   │             ▼             │                                                         │
│   │   ┌───────────────────┐   │  idempotent Lua       ┌─────────────────────────────┐   │
│   │   │   RequestWorker   │ ─ ┼ ────────────────────> │ Stream: queue:connector:rss │   │
│   │   │  (Atomic Claim)   │   │   _IDEMPOTENT_XADD    └──────────────┬──────────────┘   │
│   │   └───────────────────┘   │                                      │                  │
│   │             ▲             │                                      │                  │
│   │             │ updates     │                                      │                  │
│   │             │ status      │                                      │                  │
│   │   ┌─────────┴─────────┐   │  dual loops:          ┌──────────────┴──────────────┐   │
│   │   │   ResultWorker    │ < ┼ ───────────────────── │ Stream:                     │   │
│   │   │ (Ingest & Dedupe) │   │  - new messages (>)   │ connector:rss:results       │   │
│   │   └─────────┬─────────┘   │  - xautoclaim PEL     └──────────────▲──────────────┘   │
│   │             │             │    recovery                  │                          │
│   │             ▼             │                              │                          │
│   │      [rss_items: doc]     │                              │                          │
│   │    (dedup on item_hash)   │                              │                          │
│   └───────────────────────────┘                              │                          │
└──────────────────────────────────────────────────────────────┼──────────────────────────┘
                                                               │
┌──────────────────────────────────────────────────────────────┼──────────────────────────┐
│                                      ARGUS CONNECTOR         │                          │
│                                                              │                          │
│   ┌───────────────────────────┐                              │                          │
│   │      ConnectorWorker      │ <────────────────────────────┘                          │
│   │  - XREADGROUP & XAUTOCLAIM│   pulls crawl tasks                                     │
│   │  - Dynamic Connector      │                                                         │
│   │  - Async HTTP fetch/parse │                                                         │
│   │  - Publishes CrawlResult  │ ────────────────────────────────────────────────────────┘
│   │  - ACKs original task     │   publishes completed results
│   └───────────────────────────┘
│                                                                                         │
│   * Completely stateless: Has NO credentials or connection to MongoDB.                  │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

#### Detailed Pipeline Flow

1. **Candidate Due Detection:**
   `SchedulerWorker` triggers every `dispatch_tick_seconds` (default 5s). It sweeps `schedule:active_keys` and queries `schedule:due:{platform}:{priority}` for units whose score $\le \text{now}$.
2. **Durable Request Creation:**
   Instead of publishing directly to Redis Streams, the scheduler invokes `create_scheduled_rss_request()` in `scheduled_request_service.py`. This writes a durable document to MongoDB's `rss_requests` with `status="pending"` and deterministic ID `scheduled:{unit_id}:{scheduled_timestamp:.6f}`. Retries are safely absorbed by MongoDB's unique index.
3. **Compare-And-Swap Score Advancement:**
   Only after MongoDB confirms the request exists, the scheduler calculates `next_run_at` and executes the atomic Lua script `_ADVANCE_DUE_TIME_SCRIPT`. This script advances the sorted set score **only if the current score matches the expected due score**, guaranteeing that no concurrent dispatcher clobbers a newer runtime score.
4. **Outbound Dispatch:**
   The existing `RequestWorker` daemon continuously claims `pending` requests from MongoDB `rss_requests` (transitioning status to `processing`), and dispatches them to Redis Stream `queue:connector:rss` via `_IDEMPOTENT_XADD_SCRIPT`.
5. **Connector Crawling:**
   `ConnectorWorker` pulls tasks from `queue:connector:rss` using consumer group `rss_connectors`. It dynamically instantiates the appropriate connector (e.g. `RSSConnector`), scrapes the target URL, extracts articles, publishes a `CrawlResult` to `connector:rss:results`, and acknowledges (`xack`) the task stream message.
6. **Inbound Result Processing & Deduplication:**
   `ResultWorker` reads from `connector:rss:results` using consumer group `orchestrator-results`. It parses the payload, executes `result_service.process_crawl_result()`, performs bulk upserts on `rss_items` using `item_hash` deduplication, updates the request status in `rss_requests` to `completed` or `failed`, and calls `xack` **strictly after persistence succeeds**.
7. **Pending Message Recovery:**
   If a worker crashes before calling `xack`, the message remains in the Pending Entries List (PEL). A background recovery loop running in `ResultWorker` uses `xautoclaim` with `claim_idle_ms=30000` to reclaim and reprocess stale results.

---

### 9.2 Scheduler Design & Redis Hot-Path Data Structures

To support sub-millisecond candidate discovery without querying MongoDB on the tick loop, the scheduler maintains runtime scheduling state in Redis:

```text
Redis Keyspace:
┌──────────────────────────────────────┐
│ Set: 'schedule:active_keys'          │ ──> Members: '{platform}:{priority}' combinations (e.g., 'rss:high')
└──────────────────────────────────────┘
┌──────────────────────────────────────┐
│ Set: 'schedule:managed_unit_ids'     │ ──> Members: unit_id strings managed by MongoDB sync
└──────────────────────────────────────┘
┌──────────────────────────────────────┐
│ Hash: 'unit:{unit_id}'               │ ──> Metadata: term, platform, priority, cadence_seconds,
└──────────────────────────────────────┘     schedule_type, timezone_name, run_times, org_id, search_options, enabled
┌──────────────────────────────────────┐
│ Sorted Set:                          │ ──> Member: unit_id
│ 'schedule:due:{platform}:{priority}' │     Score:  Unix epoch float timestamp
└──────────────────────────────────────┘
```

#### Field Specifications for `unit:{unit_id}` (Redis Hash):

| Field | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `term` | `str` | Target query term or entity | `"copper prices"` |
| `platform` | `str` | Platform identifier | `"rss"` |
| `priority` | `str` | Scheduling priority tier | `"high"`, `"normal"` |
| `cadence_seconds` | `str(int)` | Interval duration in seconds | `"300"` |
| `schedule_type` | `str` | `"interval"` or `"daily_times"` | `"daily_times"` |
| `timezone_name` | `str` | Target timezone for daily schedules | `"Asia/Kolkata"` |
| `run_times` | `str(JSON)` | List of 24h clock strings | `'["10:00", "19:00"]'` |
| `org_id` | `str` | Organization / tenant ID | `"org-1"` |
| `search_options` | `str(JSON)` | Additional connector parameters | `'{"language": "en"}'` |
| `enabled` | `str(bool)` | Active flag (`"true"` / `"false"`) | `"true"` |

#### Polling Interval vs Configured Schedule Times
A vital architectural distinction:
- **`dispatch_tick_seconds` (default 5s):** The polling frequency of the `SchedulerWorker`. It simply tells the orchestrator how often to wake up, synchronize definitions, and check if *any* unit has reached its due timestamp.
- **Configured Schedule Times:** The business run schedule for a unit. For interval units, this is every `cadence_seconds` (e.g. 300s). For daily units, this is specific clock times (e.g., 10:00 AM and 7:00 PM IST).

---

### 9.3 Two-Phase Dispatch & Compare-And-Swap (CAS) Lua

#### Why the Prototype's Direct Lua XADD Was Replaced
In earlier iterations, the scheduler used a Lua script that directly executed `XADD` into `queue:connector:{platform}:{priority}`. This was decommissioned because:
1. **Bypassed MongoDB `rss_requests`:** Tasks were not tracked in MongoDB, losing auditability, state transitions (`pending` -> `processing` -> `completed`), and item count statistics.
2. **Stream Contract Mismatch:** Connector workers listen strictly on `queue:connector:rss`. Publishing to `queue:connector:rss:high` caused messages to sit unconsumed indefinitely.
3. **Tight Coupling:** The scheduler bypassed the orchestrator's standardized `RequestWorker` dispatch logic.

#### Current Two-Phase Architecture
The current implementation splits dispatch into two distinct phases:
- **Phase 1: Durable MongoDB Request Creation (`scheduled_request_service.py`):**
  Inserts a document into `rss_requests` with `status="pending"` and deterministic ID `scheduled:{unit_id}:{scheduled_timestamp:.6f}`.
- **Phase 2: Redis Compare-And-Swap Score Advancement (`_ADVANCE_DUE_TIME_SCRIPT`):**
  Executes an atomic CAS Lua script:
  ```lua
  local unit_id = ARGV[1]
  local expected_due_time = tonumber(ARGV[2])
  local next_run_at = tonumber(ARGV[3])

  local current_due_time = redis.call("ZSCORE", KEYS[1], unit_id)

  if not current_due_time then
      return 0
  end

  if tonumber(current_due_time) ~= expected_due_time then
      return 0
  end

  redis.call("ZADD", KEYS[1], next_run_at, unit_id)

  return 1
  ```
  If another concurrent worker advanced the unit's score during Phase 1, `ZSCORE ~= expected_due_time`, CAS returns `0`, and the newer score is preserved without modification.

---

### 9.4 Daily Scheduling & Timezone Calculations

Daily scheduling is handled by `app/services/schedule_time_service.py`:

```python
calculate_next_run(now: datetime, run_times: list[str], timezone_name: str = "Asia/Kolkata") -> datetime
```

#### Calculation Rules & Edge Cases
1. **Clock Times:** Configured as naive 24-hour clock strings (e.g. `["10:00", "19:00"]`). Rejects timezone-aware strings, duplicates, or empty lists.
2. **Timezone Conversion:** Uses `zoneinfo.ZoneInfo("Asia/Kolkata")` to convert UTC `now` to Indian Standard Time (IST).
3. **Intraday Evaluation:**
   - At 09:45 IST -> next run is today at 10:00 IST (04:30 UTC).
   - At 10:00 IST (exact boundary) -> next run advances to today at 19:00 IST (13:30 UTC).
   - At 10:05 IST -> next run is today at 19:00 IST (13:30 UTC).
4. **Day Rollover:**
   - At 19:05 IST (after the last run of the day) -> adds 1 day to local date and selects the earliest configured time: tomorrow at 10:00 IST (04:30 UTC next day).
5. **UTC Return:** Always converts the calculated local datetime back to UTC for consistent Unix timestamps in Redis.

---

### 9.5 MongoDB-to-Redis Synchronization & Reconciliation

Durable schedule definitions reside in MongoDB's `scheduled_units` collection. Synchronization into Redis is executed by `sync_scheduled_units_from_mongodb()`:

1. **Read-First Safety:** Reads MongoDB `scheduled_units`. If MongoDB is down or queries fail, Redis is left completely untouched.
2. **Preserving Runtime Due Times (`preserve_due_time=True`):**
   When updating Redis, it calls `pipe.zadd(due_key, {unit_id: _timestamp(next_run_at)}, nx=True)`.
   The `nx=True` flag ensures that **only new units receive the MongoDB initial due time**. For units already running in Redis, existing runtime due scores are preserved and never overwritten with stale MongoDB values.
3. **Stale Schedule Reconciliation:**
   Tracks all MongoDB-managed IDs in `schedule:managed_unit_ids`. Computes:
   ```python
   stale_ids = (existing_managed_ids - seen_ids) | disabled_ids
   ```
   For every stale ID, an atomic pipeline deletes `unit:{unit_id}`, removes it from `schedule:due:...`, and removes it from `schedule:managed_unit_ids`.

---

### 9.6 Inbound Result Worker & `XAUTOCLAIM` Recovery

`ResultWorker` runs two concurrent `asyncio` loops:

1. **`_consumer_new_messages()`:**
   Consumes new undelivered messages from `connector:rss:results` using `xreadgroup` with ID `">"`.
2. **`_recover_pending_messages()`:**
   Periodically calls `xautoclaim`:
   ```python
   result = await redis_client.xautoclaim(
       name=self.stream_name,
       groupname=self.group_name,
       consumername=self.recovery_consumer_name,
       min_idle_time=self.claim_idle_ms,  # 30,000 ms (30s)
       start_id=claim_start_id,
       count=self.batch_size,
   )
   ```
   Scans the consumer group's PEL for messages idle for more than 30 seconds. Reclaims them under consumer name `{consumer_name}-recovery` and re-processes them. When scan reaches `"0-0"`, waits for `recovery_interval` (10s) before starting the next scan.
3. **Strict Post-Persistence Acknowledgement (`xack`):**
   `xack` is called **only after** `process_crawl_result()` succeeds in MongoDB. If an error occurs, the message remains in the PEL and is reclaimed by the recovery loop.

---

### 9.7 Comprehensive Test Verification Matrix

All 54 tests in `argus-orchestrator` across the 16 test files matching `test_*.py` pass cleanly (historical baseline: 42 tests passed prior to adding stale request recovery):

```powershell
.\venv\Scripts\python.exe -m unittest discover -s app/test -p "test_*.py" -v
# Output: Ran 54 tests in 2.044s ... OK
# Historical Output (prior to request recovery): Ran 42 tests in 2.700s ... OK
```

| Test File | Test Type | Infrastructure Dependencies | Scope & Description |
| :--- | :--- | :--- | :--- |
| `test_schedule_time_service.py` | Unit Test | None (Pure Python) | Tests clock time parsing, intra-day candidate advancement, day rollover, and error handling for daily schedules in `Asia/Kolkata`. |
| `test_scheduled_request_service.py` | Mocked Integration | None (Mocked MongoDB) | Verifies durable MongoDB request document creation, deterministic request IDs, and retry tolerance via `DuplicateKeyError`. |
| `test_scheduler_dispatch.py` | Mocked Integration | None (Mocked Redis & Mongo) | Tests two-phase dispatch, CAS score advancement sequence, CAS rejection handling, and daily schedule next-run integration. |
| `test_scheduler_sync.py` | Mocked Integration | None (Mocked Redis & Mongo) | Tests synchronization of active schedules, validation of malformed fields, and purge of disabled or deleted units. |
| `test_scheduler_sync_due_time.py` | Real Integration | Real Redis (Port 6379), Mocked Mongo | Proves that `sync_scheduled_units_from_mongodb()` with `nx=True` preserves existing runtime due scores in Redis. |
| `test_scheduler_tick.py` | Real Integration | Real Redis (Port 6379), Mocked Mongo | Tests multi-combination sweeps across `schedule:active_keys`, score advancement, and repeat dispatch prevention. |
| `test_scheduler_worker.py` | Unit Test | None (Mocked services) | Verifies synchronization-before-dispatch execution order, periodic ticking, and clean graceful shutdown. |
| `test_scheduler_service.py` | Real Integration | Real Redis (Port 6379) | Verifies registration, hash metadata persistence, and time-based candidate lookups against real Redis sorted sets. |
| `test_result_worker.py` | Unit Test | None (Mocked Redis & Mongo) | Verifies post-persistence `xack`, suppression of `xack` on failure, and `XAUTOCLAIM` recovery invocation. |
| `manual_result_worker_recovery.py` | Standalone Integration | Real Redis (Port 6379), Mocked Mongo | Standalone manual verification script (does not match `test_*.py`); verifies `XAUTOCLAIM` reclamation, processing, and PEL clearance against a temporary isolated Redis test stream. |
| `test_request_service.py` | Mocked Integration | None (Mocked Redis & Mongo) | Verifies outbound request dispatch via `_IDEMPOTENT_XADD_SCRIPT`, metadata serialization, audit updates, and retry deduplication. |
| `test_redis_dispatch_recovery.py` | Real Integration | Real Redis (Port 6379/6380) | Proves that `_IDEMPOTENT_XADD_SCRIPT` detects deleted stream entries via `XRANGE` and republishes replacements cleanly. |
| `test_request_recovery.py` | Mocked Integration & Unit | None (Mocked Mongo & Redis) | Verifies stale request atomic recovery claims, recovery leases, Redis message ID reconciliation, and missing `request_id` handling. |
| `test_request_worker.py` | Unit Test | None (Mocked services) | Verifies `RequestWorker` startup health checks, periodic stale request recovery invocation, and recovery exception isolation. |
| `test_result_service.py` | Mocked Integration | None (Mocked Redis & Mongo) | Tests article deduplication on `item_hash`, in-batch filtering, Redis `mget` cache, Redis failure fallback, pipeline TTL, and status updates. |
| `test_request_idempotency.py` | Real Integration | Real MongoDB | Tests concurrent worker races to insert identical queries; verifies partial unique index enforcement. |
| `test_bing.py` | Standalone Integration | Live Network | Verifies Bing RSS search endpoint construction and feed XML parsing. |

---

### 9.8 Resolution of Prototype Defects (Register 1–8)

During initial prototyping, eight defects were identified in the scheduler subsystem. All eight have been comprehensively resolved:

| Defect # | Original Defect Description | Resolved Implementation in Current Code | Resolution Evidence |
| :--- | :--- | :--- | :--- |
| **Defect 1** | `ACTIVE_KEYS_SET` stored `unit:{unit_id}` instead of `{platform}:{priority}`. | [`scheduler_service.py`](file:///C:/Users/OmMangeshBangar/PycharmProjects/argus-2.0/argus-orchestrator/app/services/scheduler_service.py#L128) executes `pipe.sadd(ACTIVE_KEYS_SET, f"{platform}:{priority}")`. | Sweep loop correctly extracts `platform` and `priority`. |
| **Defect 2** | `dispatch_due_units_once` had loop variable shadowing and indented `return`. | Variable renamed to `for active_key in sorted(active_keys):` and `return dispatched_count` dedented outside the loop. | Sweeps iterate over all active combinations. Verified in `test_scheduler_tick.py`. |
| **Defect 3** | Typo in test cleanup key `"schedule:active_key"` without 's'. | Corrected to `"schedule:active_keys"` in [`test_scheduler_service.py`](file:///C:/Users/OmMangeshBangar/PycharmProjects/argus-2.0/argus-orchestrator/app/test/test_scheduler_service.py#L72). | Zero orphaned keys remain in Redis after test execution. |
| **Defect 4** | Key prefix inconsistency (`scheduler:due:...` vs `schedule:due:...`). | Standardized to `schedule:due:{platform}:{priority}` across all services, Lua scripts, and tests. | Clean, consistent Redis keyspace naming. |
| **Defect 5** | Singular naming `get_due_unit_id()` returning a list. | Maintained signature for backwards compatibility while returning `list[str]`; documented clearly. | All callers handle candidate list properly. |
| **Defect 6** | Suppressed exception traceback in `SchedulerWorker.run()`. | Replaced `logger.info(...)` with `logger.exception("Scheduler tick failed.")`. | Full exception tracebacks and error contexts are logged. |
| **Defect 7** | Dead `scheduled_units` import in `scheduler_service.py`. | Wired into `sync_scheduled_units_from_mongodb()` for full MongoDB-to-Redis hydration and reconciliation. | Active database-to-cache sync mechanism. |
| **Defect 8** | Direct Lua `XADD` stream contract mismatch (`queue:connector:rss:high` vs `queue:connector:rss`). | Replaced direct stream publishing with two-phase dispatch: creates durable MongoDB request in `rss_requests`, dispatched by `RequestWorker`. | Perfectly aligns with connector contract and preserves audit trail. |

---

### 9.9 Outstanding Work, Verification Gaps & Production Readiness

While the scheduling and recovery subsystems are feature-complete and fully unit/integration tested, the following items require live operational verification before production deployment:

1. **Initial Registration of Daily Schedules:**
   A seeding script or admin API to create new daily schedule documents in MongoDB `scheduled_units` and verify that the very first calculated due time in Redis matches expectations.
2. **Real Full End-to-End Scheduled Crawl Pipeline:**
   Running all three services (`argus-orchestrator`, `argus-connector`, `redis`, `mongodb`) live and letting a scheduled unit fire, traverse the request worker, crawl Bing News RSS, publish results, and persist to `rss_items`.
3. **Connector Multi-Source Scheduled Dispatch:**
   Currently, `scheduled_request_service.py` builds URLs exclusively via `build_bing_rss_url(term)`. Expanding the scheduler to trigger NSE circulars, NSE announcements, or BSE crawls requires adding platform-specific request builders or payload models.
4. **Distributed Scheduler Lock for Horizontal Scaling:**
   `SchedulerWorker` runs inside `argus-orchestrator`. If multiple orchestrator instances are deployed in a cluster, a Redis-based distributed mutex (`SET lock:scheduler_tick <id> NX EX 5`) should be implemented so that only the leader instance runs the sweep tick.

---

### 9.10 Redis Article Deduplication Architecture & Stream Recovery Semantics

This subsection details the two production-hardening mechanisms implemented across `result_service.py` and `request_service.py`:

#### 1. Multi-Tier Article Deduplication Pipeline
To protect MongoDB from write amplification when crawlers ingest recurring RSS feeds, `result_service.py` executes a three-tier deduplication filter:

```text
Incoming CrawlResult (Items)
            │
            ▼
┌───────────────────────────────────────┐
│ Tier 1: In-Batch Set Deduplication    │ ──> Discards intra-batch duplicates (seen_hashes)
└───────────────────┬───────────────────┘
                    │ (unique_items)
                    ▼
┌───────────────────────────────────────┐
│ Tier 2: Redis Cache MGET Lookup       │
│ Key: 'argus:article:seen:{item_hash}' │ ──> Filters cached items seen within 30 days
│ (Fallback: MongoDB on Redis Error)    │
└───────────────────┬───────────────────┘
                    │ (unseen_items)
                    ▼
┌───────────────────────────────────────┐
│ Tier 3: MongoDB Authoritative Upsert  │
│ rss_items.bulk_write([$setOnInsert])  │ ──> Atomic document insertion & index safety net
└───────────────────┬───────────────────┘
                    │
                    ▼
┌───────────────────────────────────────┐
│ Post-Save Redis Pipeline Caching      │
│ pipeline.set(..., "1", ex=2592000)    │ ──> Populates 30-day cache for new items
└───────────────────────────────────────┘
```

**Key Operational Properties:**
- **Redis TTL (30 Days):** Caches article fingerprints (`ARTICLE_SEEN_PREFIX = "argus:article:seen:"`) with `ex=30*24*60*60` (2,592,000 seconds). High temporal relevance while preventing unbounded Redis memory growth.
- **Fail-Open Resilience:** If Redis is down, `filter_unseen_items()` catches the exception, logs a warning with traceback, and returns all items to proceed with MongoDB insertion. System availability is never sacrificed for caching.
- **Comprehensive Metrics:** Accurate tracking of `total_duplicates = stats["duplicates"] + redis_duplicates + batch_duplicates`, providing full visibility into crawler deduplication efficiency.

#### 2. Stream Deletion & Eviction Self-Healing (`_IDEMPOTENT_XADD_SCRIPT`)
Under Redis memory pressure or manual stream administration (`XDEL`, `XTRIM MAXLEN`), stream entries can disappear while per-request deduplication keys (`argus:dispatch:request:{request_id}:stream_message_id`) remain in Redis.

**Self-Healing Protocol:**
1. Check `GET KEYS[2]`. If `existing_id` is found, run `XRANGE KEYS[1] existing_id existing_id COUNT 1`.
2. If `#existing_message > 0`, the message is active in the stream; return `existing_id` immediately without publishing.
3. If `#existing_message == 0`, the stream entry was deleted or evicted. Bypass deduplication, publish a fresh message via `XADD`, and overwrite `KEYS[2]` with the new message ID.
4. Subsequent retries find the replacement message in the stream and return its ID idempotently.

This protocol completely eliminates orphaned dispatch deadlocks in distributed production environments.

---

## 10. Implementation Guide: Stale Request Recovery, Idempotent Dispatch & Worker Reconciliation

This section documents the architecture, implementation, configuration, verification, and open design review items for outbound request recovery in `argus-orchestrator`.

---

### 10.1 Request Dispatch and Recovery Architecture

The request pipeline connects MongoDB and Redis with a strict separation of concerns:
- **MongoDB as Durable Source of Truth:** All crawl requests originate and are tracked in the MongoDB `rss_requests` collection. MongoDB maintains the authoritative lifecycle status (`pending`, `processing`, `completed`, `failed`), tracking timestamps (`created_at`, `claimed_at`, `dispatched_at`, `completed_at`, `updated_at`), the allocated Redis stream entry identifier (`stream_message_id`), and any diagnostic error strings (`error_message`).
- **Request Worker Outbound Dispatch:** The `RequestWorker` daemon queries for `pending` requests in MongoDB, claims a single document atomically (`status: "processing"`, `claimed_at = now`), constructs a `ConnectorRequest` payload, and publishes it to the configured Redis connector stream (`queue:connector:rss`).
- **Why Processing Requests Can Become Stale (Audit Write Failure):**
  In `request_service.dispatch_request()`, the Redis stream publication occurs first via `_publish_request_idempotently()`. If Redis successfully accepts the message and issues a `stream_message_id`, but the subsequent MongoDB audit update (`update_one` setting `stream_message_id` and `dispatched_at`) fails (e.g. temporary MongoDB disconnection, socket timeout, primary replica step-down, or worker process crash), the request document in MongoDB remains in `status: "processing"`, `claimed_at` set in the past, and `dispatched_at: None`.
  Because standard dispatch only polls for `status: "pending"`, the request becomes stranded in `processing`. The downstream connector may already be crawling the target or may have completed it, but the orchestrator cannot correlate the job without `stream_message_id` and `dispatched_at`.
- **How Stale Request Recovery Reconciles State:**
  The background function `recover_stale_processing_requests()` continuously resolves this distributed inconsistency. It identifies requests stuck in `processing` beyond a configurable timeout threshold, places a temporary atomic recovery lease on them to prevent concurrent worker contention, reconciles the Redis stream state (reusing existing stream entries or republishing idempotently), and updates MongoDB with the resolved `stream_message_id` and `dispatched_at`.

---

### 10.2 Idempotent Redis Publishing & Deduplication Keys

To ensure that retries and recovery runs do not flood downstream connectors with redundant work, Redis stream publication is strictly idempotent.

#### 1. Deduplication Key Format
Every outbound request uses a deterministic deduplication key in Redis:
```text
argus:dispatch:request:{request_id}:stream_message_id
```
This key maps the logical request identifier (e.g. `scheduled:unit-001:1710000000.000000` or an ad-hoc UUID) directly to the assigned Redis stream message ID (e.g. `1710000000123-0`).

#### 2. Atomic Stream Publication (`_publish_request_idempotently`)
The service executes `_IDEMPOTENT_XADD_SCRIPT` passing `KEYS[1]` as `settings.connector_stream` and `KEYS[2]` as the deduplication key:
```lua
local existing_id = redis.call("GET", KEYS[2])

if existing_id then
    local existing_message = redis.call(
        "XRANGE",
        KEYS[1],
        existing_id,
        existing_id,
        "COUNT",
        1
    )

    if #existing_message > 0 then
        return existing_id
    end
end

local message_id = redis.call(
    "XADD",
    KEYS[1],
    "*",
    unpack(ARGV)
)

redis.call("SET", KEYS[2], message_id)

return message_id
```
**Decision Flow:**
1. Check if a deduplication key exists via `GET KEYS[2]`.
2. If `existing_id` is present, inspect the stream with `XRANGE` to verify that the message still physically exists in the stream.
3. If `#existing_message > 0`, return `existing_id` immediately without publishing.
4. If `#existing_message == 0` (e.g. the entry was truncated or removed with `XDEL`), publish a replacement message via `XADD`, update the deduplication key with the new ID, and return it.

#### 3. Pre-Dispatch Message Verification (`_get_existing_stream_message_id`)
Before deciding whether to republish during recovery, `_get_existing_stream_message_id(request_id)` queries Redis:
```python
async def _get_existing_stream_message_id(request_id: str) -> Optional[str]:
    dedupe_key = f"argus:dispatch:request:{request_id}:stream_message_id"

    message_id = await redis_client.get(dedupe_key)
    if not message_id:
        return None

    entries = await redis_client.xrange(
        settings.connector_stream,
        min=message_id,
        max=message_id,
        count=1,
    )

    if not entries:
        return None

    return str(message_id)
```
If both the deduplication key and the stream entry are intact, the existing `message_id` is reused directly, bypassing republishing entirely.

#### 4. Known Limitation: Key Eviction Edge Case
If the Redis deduplication key `argus:dispatch:request:{request_id}:stream_message_id` has been evicted or expired while an older message entry still exists in `queue:connector:rss`, `_get_existing_stream_message_id()` returns `None`. When `_publish_request_idempotently()` is subsequently invoked, it will also find no deduplication key and will execute `XADD` to create a fresh stream entry.
In this specific scenario, a duplicate stream message will be published to the connector. This edge case remains pending architectural review; the system provides **at-least-once delivery with best-effort stream deduplication**, not complete exactly-once delivery.

---

### 10.3 Stale Request Recovery Implementation (`recover_stale_processing_requests`)

The recovery routine operates in five distinct phases:

#### 1. Candidate Eligibility Query
A request is eligible for recovery if:
- `status == "processing"`
- `claimed_at <= (now - timeout_seconds)` (where `timeout_seconds` defaults to `settings.request_recovery_timeout_seconds`, 120s)
- `dispatched_at is None` (proves the original audit write never completed)

#### 2. Atomic Recovery Claim Lease
To prevent multiple orchestrator instances from claiming the same stale request simultaneously, recovery uses a distributed lease mechanism:
```python
now = datetime.now(timezone.utc)
cutoff = now - timedelta(seconds=timeout_seconds)
lease_cutoff = now - timedelta(seconds=lease_seconds)
recovery_token = str(uuid.uuid4())

stale_request = await rss_requests.find_one_and_update(
    {
        "status": "processing",
        "claimed_at": {"$lte": cutoff},
        "dispatched_at": None,
        "$or": [
            {"recovery_claimed_at": {"$exists": False}},
            {"recovery_claimed_at": None},
            {"recovery_claimed_at": {"$lte": lease_cutoff}},
        ],
    },
    {
        "$set": {
            "recovery_claimed_at": now,
            "recovery_token": recovery_token,
            "updated_at": now,
        }
    },
    return_document=ReturnDocument.AFTER,
)
```
If an active lease exists (`recovery_claimed_at > lease_cutoff`), competing workers skip the request until `lease_seconds` (120s) has elapsed.

#### 3. Handling Requests Missing `request_id`
If a claimed document does not possess a valid `request_id`, it cannot be correlated with Redis. The routine immediately releases the recovery lease so the document is not permanently locked:
```python
if not request_id:
    logger.error("Stale MongoDB request %s has no request_id.", stale_request.get("_id"))
    await rss_requests.update_one(
        {
            "_id": stale_request["_id"],
            "status": "processing",
            "recovery_token": recovery_token,
        },
        {
            "$unset": {"recovery_claimed_at": "", "recovery_token": ""},
            "$set": {"updated_at": datetime.now(timezone.utc)},
        },
    )
    return 0
```

#### 4. Redis Stream Reconciliation
1. Checks `message_id = await _get_existing_stream_message_id(request_id)`.
2. If `message_id` is found, reuses it without publishing.
3. If no message exists, constructs `ConnectorRequest` and publishes via `_publish_request_idempotently(request_id, payload)`.

#### 5. Guarded MongoDB Reconciliation & Exception Safety
The update back to MongoDB is strictly guarded by the `recovery_token`:
```python
recovered_at = datetime.now(timezone.utc)
update_result = await rss_requests.update_one(
    {
        "_id": stale_request["_id"],
        "status": "processing",
        "recovery_token": recovery_token,
        "dispatched_at": None,
    },
    {
        "$set": {
            "stream_message_id": message_id,
            "dispatched_at": recovered_at,
            "updated_at": recovered_at,
            "error_message": None,
        },
        "$unset": {
            "recovery_claimed_at": "",
            "recovery_token": "",
        },
    },
)
```
If an unexpected exception occurs during recovery, the outer `except Exception` block catches it, logs the traceback with `logger.exception(...)`, and attempts an emergency release of the recovery lease (`$unset: {"recovery_claimed_at": "", "recovery_token": ""}`) so subsequent cycles can retry.

**Distributed Transaction Limitation:**
A distributed transaction spanning Redis and MongoDB is not supported in this architecture. If the process crashes between Redis publication and MongoDB reconciliation, the recovery cycle relies on the deduplication key on the next lease iteration to discover the published message.

---

### 10.4 Request Worker Integration

The `RequestWorker` in `app/workers/request_worker.py` integrates recovery into its main event loop:
1. Maintains an in-memory execution timestamp: `self._last_recovery_time = 0.0`.
2. In each iteration of the `while self._is_running` processing loop:
   ```python
   now = asyncio.get_running_loop().time()
   recovery_interval = settings.request_recovery_interval

   if now - self._last_recovery_time >= recovery_interval:
       self._last_recovery_time = now
       try:
           recovered = await recover_stale_processing_requests()
           if recovered:
               logger.info("Recovered %s stale request(s).", recovered)
       except Exception:
           logger.exception("Request recovery failed; normal dispatch will continue.")
   ```
3. **Fault Isolation:** If recovery fails, the exception is logged with full context and the worker immediately proceeds to normal outbound dispatch (`await dispatch_request()`).
4. **Standard Dispatch Mechanics:** The worker continues to drain pending requests with 10ms intervals during backlogs, and backs off to `poll_interval` (2s) via `asyncio.wait_for` when the queue is empty.

---

### 10.5 Configuration & Pydantic Validation

The orchestrator settings in `app/core/config.py` define the operational parameters:
```python
request_recovery_interval: int = 10         # Seconds between periodic recovery scans
request_recovery_timeout_seconds: int = 120 # Minimum age of claimed_at before a request is stale
request_recovery_lease_seconds: int = 120   # Duration of the recovery lease lock
```

#### Strict Pydantic Field Validation
A Pydantic `@field_validator` ensures that all three recovery settings are strictly positive integers:
```python
@field_validator(
    "request_recovery_interval",
    "request_recovery_timeout_seconds",
    "request_recovery_lease_seconds",
)
@classmethod
def validate_request_recovery_settings(cls, value: int) -> int:
    if value <= 0:
        raise ValueError("Request recovery settings must be greater than zero.")
    return value
```
Instantiating `Settings` with zero or negative values (e.g. `Settings(request_recovery_interval=0)`) immediately raises a Pydantic `ValidationError`, preventing invalid startup states.

---

### 10.6 Tests and Current Verification Status

The recovery subsystem has been verified through focused unit and regression test runs:

1. **Configuration Defaults & Validator Verification:**
   - Default values confirmed: `request_recovery_interval = 10`, `request_recovery_timeout_seconds = 120`, `request_recovery_lease_seconds = 120`.
   - Verified that `Settings(request_recovery_interval=0)` raises `pydantic_core._pydantic_core.ValidationError`.

2. **Core Recovery Test Suite (`test_request_recovery.py`):**
   - `test_no_eligible_request_returns_zero`: Confirms no action when no processing requests are stale.
   - `test_stale_request_is_claimed_atomically`: Verifies atomic lease acquisition and token assignment.
   - `test_invalid_timeout_is_rejected`: Confirms `ValueError` on zero or negative timeout arguments.
   - `test_active_lease_condition_is_in_query`: Confirms that unexpired recovery leases prevent duplicate claims.
   - `test_recovery_claim_does_not_publish_without_reconciliation`: Confirms that existing Redis stream entries are reused without calling publish.
   - `test_existing_stream_message_id_found`: Confirms `_get_existing_stream_message_id` returns message ID when present in both key and stream.
   - `test_existing_stream_message_id_missing_from_stream`: Confirms `None` returned when key exists but stream entry was removed.
   - `test_existing_stream_message_id_without_dedupe_key`: Confirms `None` returned when dedupe key is absent.
   - `test_missing_request_id_releases_recovery_claim`: Confirms that a stale request missing `request_id` is aborted and unsets `recovery_claimed_at` and `recovery_token`.

3. **Request Worker Integration Suite (`test_request_worker.py`):**
   - `test_worker_stops_when_health_check_fails`: Verifies clean termination if Redis/MongoDB checks fail.
   - `test_worker_attempts_recovery_and_dispatch`: Verifies that `start()` calls both recovery and dispatch.
   - `test_dispatch_continues_when_recovery_fails`: Proves that a runtime error in recovery does not prevent `dispatch_request()` from running.

4. **Full Test Suite & Multi-Suite Verification Count:**
   - **Current Full Test Suite Verification:** The complete test suite across `app/test` has been rerun end-to-end (`python -m unittest discover -s app/test -p "test_*.py"`), with **54 tests passed** cleanly in 2.044s (0 errors, 0 failures).
   - **Historical Multi-Suite Verification:**
     - Prior to the latest missing-request-ID fix, the combined test run across `test_request_worker`, `test_request_recovery`, `test_request_service`, `test_result_service`, and `test_redis_dispatch_recovery` passed **25 tests cleanly**.
     - Following the addition of `test_missing_request_id_releases_recovery_claim`, that focused test was verified independently and passed cleanly (bringing the focused recovery group to 26 tests).
     - Prior to introducing the stale request recovery subsystem, the historical orchestrator baseline was **42 tests passed**.

---

### 10.7 Remaining Work & Open Review Items

The following items are identified for the next review and iteration:
1. **Deduplication Key Eviction Edge Case:**
   Investigate mitigation strategies for when the Redis deduplication key is evicted under memory pressure while the stream entry persists in `queue:connector:rss` (to avoid duplicate publication).
2. **Regression Test Improvements:**
   Inspect and refine the Redis recovery regression tests in `test_redis_dispatch_recovery.py` and `test_request_recovery.py` to cover end-to-end timeout expiration and concurrent multi-worker recovery claim races.
3. **Test Method Renaming:**
   Evaluate renaming `test_recovery_claim_does_not_publish_without_reconciliation` in `app/test/test_request_recovery.py`. Its current assertions describe reusing an existing stream message without republishing, which is better expressed by a name such as `test_recovery_reuses_existing_stream_message_without_republishing`.
4. **Comprehensive Test Suite Rerun (Completed):**
   The comprehensive test suite rerun has been completed (`python -m unittest discover -s app/test -p "test_*.py"`), with all 54 tests passing cleanly. Continued full test suite reruns should accompany future batch modifications.
5. **Pre-Commit Git Diff Review:**
   Perform a line-by-line review of all unstaged and modified Git files (including `PROJECT_GUIDE.md`, `app/core/config.py`, `app/services/request_service.py`, `app/services/scheduler_service.py`, `app/workers/request_worker.py`, and test files) prior to staging and committing changes.

