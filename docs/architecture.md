# Pipeline Architecture & Data Flow

This document details the architecture, end-to-end data flow, and design assumptions for the API-to-Warehouse pipeline.

---

## 1. End-to-End Data Flow Diagram

```mermaid
flowchart TD
    subgraph Source ["API Source Layer"]
        API["Mock REST API (FastAPI)<br/>GET /customers<br/>GET /orders"]
    end

    subgraph Ingestion ["Ingestion & Extraction"]
        EXT["Extractor (requests)<br/>- Pagination Handling<br/>- Exponential Backoff + Jitter<br/>- Retry-After Honor (429/5xx)"]
    end

    subgraph RawLayer ["Raw Storage Layer (DuckDB)"]
        RAW_C["raw_customers<br/>(payload JSON, hash, run_id)"]
        RAW_O["raw_orders<br/>(payload JSON, hash, run_id)"]
    end

    subgraph Validation ["Validation & Quarantine"]
        VAL["Validation Engine (Pydantic)<br/>- Schema & Type Checks<br/>- Null / Corrupt Value Checks"]
        REJ[("rejected_records<br/>(run_id, entity, reason, payload)")]
    end

    subgraph Transform ["Dedup & Transformation"]
        DEDUP["SQL Transforms (DuckDB)<br/>ROW_NUMBER() OVER (<br/>PARTITION BY id ORDER BY updated_at DESC)"]
        REF["Referential Integrity Check<br/>orders.customer_id EXISTS in customers"]
    end

    subgraph CuratedLayer ["Curated Storage Layer (DuckDB)"]
        DIM_C[("dim_customer<br/>PK: customer_id")]
        FACT_O[("fact_orders<br/>PK: order_id, FK: customer_id")]
    end

    subgraph AuditAI ["Audit & AI Summary"]
        AUDIT[("pipeline_runs<br/>(metrics, counts, status)")]
        WATER[("watermarks<br/>(endpoint, last_updated_at)")]
        AI_SVC["AI Summary Service<br/>(Ollama / Rule-Based Fallback)"]
        SUMM[("run_summaries<br/>(summary, model, validated)")]
    end

    API -->|HTTP GET + Retries| EXT
    EXT --> RAW_C
    EXT --> RAW_O
    RAW_C --> VAL
    RAW_O --> VAL
    VAL -->|Invalid Records| REJ
    VAL -->|Valid Records| DEDUP
    DEDUP --> REF
    REF -->|Orphan Orders| REJ
    REF -->|Valid Records| DIM_C
    REF -->|Valid Records| FACT_O

    EXT -.->|Extraction Metrics| AUDIT
    DEDUP -.->|Load & Dedup Counts| AUDIT
    AUDIT -->|Aggregated Metrics Only| AI_SVC
    AI_SVC --> SUMM
    FACT_O -.->|Max updated_at| WATER
    DIM_C -.->|Max updated_at| WATER
```

---

## 2. Pipeline Layers

### A. Extraction Layer
- Paginated HTTP GET requests against `/customers` and `/orders`.
- Exponential backoff with random jitter for transient errors (HTTP 429, 5xx, and network timeouts).
- Honors `Retry-After` header when rate limited.
- Non-retryable client errors (HTTP 4xx except 429) fail fast.

### B. Raw Storage Layer (Bronze)
- Append-only tables: `raw_customers` and `raw_orders`.
- Stores raw payload JSON as received, along with SHA-256 `payload_hash`, timestamp, HTTP status, and `run_id`.
- Immutable audit log enabling complete replayability.

### C. Validation & Quarantine Layer
- Pydantic models validate data types, required fields, and format constraints.
- Malformed records are diverted into `rejected_records` with failure reason and original payload.
- Ingestion continues without crashing on individual row failures.

### D. Transformation & Curated Layer (Silver / Gold)
- **Deduplication:** Window functions retain the newest record per business key:
  ```sql
  ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
  ```
- **Referential Integrity:** Orders referencing non-existent `customer_id` values are quarantined into `rejected_records`.
- **Idempotency:** Upserts (`INSERT ... ON CONFLICT DO UPDATE`) guarantee re-running against identical data produces identical curated tables.

### E. Audit & AI Summary Layer
- Every execution logs runtime metrics to `pipeline_runs` (fetched, loaded, rejected, duplicates, retries, duration).
- High-watermark timestamps stored in `watermarks` enable incremental loads.
- The AI Summary Service consumes aggregated metrics (never raw PII) to produce an operational brief.
- An offline `RuleBasedProvider` acts as automatic fallback if the local LLM (Ollama) is unavailable or produces invalid metrics.

---

## 3. Assumptions & Design Constraints

1. **Business Keys:** `customer_id` and `order_id` uniquely identify records across time.
2. **Dedup Precedence:** When duplicates occur, the record with the most recent `updated_at` timestamp takes precedence.
3. **Referential Integrity:** An order cannot exist in curated storage without a matching customer. Unmatched orders are quarantined.
4. **Data Types & Currencies:** Amounts are handled as decimals with two decimal places in a single unified currency. Timestamps follow ISO-8601 UTC.
5. **Incremental Loading:** Endpoints provide `updated_since` query filtering. The pipeline tracks the latest `updated_at` per endpoint in `watermarks`.
6. **Isolated AI Path:** The AI summarizer is non-critical; pipeline execution and warehouse loading succeed regardless of AI availability.
