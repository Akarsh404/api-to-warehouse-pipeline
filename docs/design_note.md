# Design & Technical Decision Note (DAI-035)

This note documents the core architectural choices, trade-offs, and design rationale behind the API-to-Warehouse data pipeline.

---

### 1. DuckDB over Postgres / External Warehouse
- **Decision:** Embed DuckDB as the analytical warehouse engine.
- **Why:** Eliminates external server dependencies, network latency, and operational overhead during setup and local review. DuckDB provides vectorized analytical query performance, native JSON extraction, ACID transactions, and full SQL support (`ROW_NUMBER() OVER (...)`, `ON CONFLICT DO UPDATE`).
- **Trade-off:** Single-writer concurrency model. While ideal for batch ETL jobs, it is not designed for high-concurrency OLTP workloads.
- **Alternative Considered:** PostgreSQL or SQLite. Postgres requires Docker/server administration which breaches the clean-setup requirement; SQLite lacks native columnar analytical functions, window performance, and advanced JSON operators.

---

### 2. Raw Layer Stored as Append-Only JSON
- **Decision:** Ingest raw API responses directly into append-only tables (`raw_customers`, `raw_orders`) as raw JSON strings alongside an immutable SHA-256 payload hash.
- **Why:** Full operational replayability and auditability. If business rules, schemas, or downstream models change in the future, the warehouse can re-hydrate curated tables directly from the raw bronze tables without re-querying the upstream API.
- **Trade-off:** Additional storage consumption due to payload duplication and schema-on-read parsing.
- **Alternative Considered:** Direct streaming to curated tables. Discarding raw payloads leaves no audit trail and makes recovering from pipeline bugs or API contract drift impossible.

---

### 3. Mock REST API with Failure Injection over Third-Party Public API
- **Decision:** Build a deterministic local FastAPI server with synthetic data generation and configurable `fail_rate`.
- **Why:** Public APIs introduce external flakiness, rate limit quotas, auth token expirations, and unrepeatable test conditions. A dedicated mock API enables deterministic unit/integration testing of retry backoff, `Retry-After` headers, and edge cases (429, 500, corrupt data).
- **Trade-off:** Requires maintaining mock server code alongside the pipeline.
- **Alternative Considered:** Live free public APIs (e.g. JSONPlaceholder, OpenWeather). Rejected due to lack of incremental watermark support, lack of related entities linked by foreign keys, and inability to inject controlled 429/500 failures.

---

### 4. Hash + Business-Key Deduplication
- **Decision:** Use SHA-256 hashes for raw page payload validation and SQL window functions (`ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY updated_at DESC)`) for curated entity deduplication.
- **Why:** Business keys (`customer_id`, `order_id`) establish entity identity, while `updated_at` establishes state ordering. Resolving duplicates at the SQL layer ensures that the newest record always wins, regardless of ingestion arrival order.
- **Trade-off:** Requires window computation overhead in memory during staging.
- **Alternative Considered:** Python dictionary key-replacement in memory. Rejected because SQL window functions execute inside the warehouse engine, scale to millions of rows, and remain transparent to SQL audit queries.

---

### 5. Quarantine Table (`rejected_records`) over Fail-Fast
- **Decision:** Divert corrupt types, null required fields, and orphan foreign keys to `rejected_records` while allowing healthy records to load.
- **Why:** In production data engineering, a single malformed payload out of 100,000 rows should not crash an entire critical data pipeline. Quarantining isolates bad data for investigation while maintaining downstream analytical availability.
- **Trade-off:** Requires downstream monitoring and dead-letter queue triage procedures.
- **Alternative Considered:** Fail-fast (raising an immediate exception). Rejected because transient bad data would repeatedly block valid transactions and breach SLAs.

---

### 6. AI Limited to Summarizing Aggregated Metrics
- **Decision:** Isolate the AI Summarizer outside the critical data path and restrict its input strictly to aggregated operational metrics.
- **Why:** Data security, latency, and reliability. Pipeline execution must never stall if an LLM is offline. Furthermore, customer PII (names, emails) must never be sent to LLM endpoints. A deterministic `RuleBasedProvider` guarantees immediate fallback.
- **Trade-off:** The LLM cannot perform granular row-level data quality remediation.
- **Alternative Considered:** Using an LLM directly in the ETL cleaning path (e.g. fixing typos). Rejected due to hallucination risks, slow latency, cost, and violation of the prompt isolation constraint.
