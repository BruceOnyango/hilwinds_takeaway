## 3. dbt System Design  
*(Based on research — I have not used dbt professionally, so this design reflects best practices from dbt documentation and community examples.)*  

### 3.1 Model Layers (Staging → Intermediate → Fact)

- **Staging (`stg_*`)**
  - Load raw CSVs using `source()`, standardize column names/types, clean duplicates.
  - Parse dates, normalize emails, extract structured fields from notes.
  - Attach `source_file`, `_ingested_at`, and `_batch_id` for auditability.
  - Standardize EIN formats, cast booleans, and fix null representations.

- **Intermediate (`int_*`)**
  - Join employees ↔ plans ↔ claims.
  - Apply business logic (EIN inference, deriving tenure, normalizing company domains).
  - Enrich with company metadata (industry, headcount, revenue).
  - Apply row-grain checks to ensure stable keys.
  - Prepare surrogate keys for fact tables.

- **Fact Models (`fct_*`)**
  - Create analytics-ready tables: `fct_employees`, `fct_plans`, `fct_claims`.
  - Maintain stable grain (e.g., 1 row per employee, 1 row per plan, 1 row per claim).
  - Use `incremental` materialization to process only new or updated rows.
  - Partition large facts by date (`service_date`, `start_date`) for performance + cost efficiency.
  - Add derived metrics (e.g., employee tenure, claim age).

---

### 3.2 Incremental Merge Logic (did not really understand this part)


## 4. Deep Debugging & Incident Response  
*(Scenario: a new feed adds 20M rows/day; costs triple; dbt models runtime jumps 4m → 45m; outputs are incorrect despite passing dbt tests.)*

### 4.1 Immediate symptoms
- Sudden surge in input volume (20M rows/day).
- Query runtime and compute cost spike.
- Final outputs show incorrect aggregates or duplicates, but schema-level dbt tests pass.

---

### 4.2 Likely root causes
- **Partitioning / clustering misalignment** — queries scan full tables instead of pruning partitions.  
- **Key explosion (join cardinality)** — bad join keys or many-to-many joins create massively inflated intermediate results.  
- **Duplicate / backfilled data** — source feed contains duplicates or repeated partitions; merge logic not deduping.  
- **Incremental merge predicate failure** — `is_incremental()` filter or `unique_key` incorrect, causing reprocessing or missed updates.  
- **Schema drift or unexpected nulls** — business logic assumes fields that are now malformed/empty.  
- **Tests are structural, not behavioral** — dbt tests catch schema issues but not business-rule regressions (e.g., double-counting).

---

### 4.3 How to isolate the regression (stepwise)
1. **Snapshot counts**: capture row counts and key cardinalities at each layer (raw → stg → int → fct) and compare to last good run.  
2. **Identify the expansion point**: run each intermediate model independently (smallest → largest) and observe where row-counts or sizes explode.  
3. **Profile queries**: use warehouse query profile / EXPLAIN to find scans, broadcast joins, or memory-heavy steps.  
4. **Check incremental predicates**: verify `last_updated`/HWM logic and unique keys used in MERGE are correct and selective.  
5. **Sample data inspection**: pull a small sample of rows around the problematic keys to inspect duplicates, nulls, and unexpected values.  
6. **Compare full-refresh vs incremental**: run a `--full-refresh` on the suspect model to see whether incremental logic or data shape is the cause.

---

### 4.4 Detecting logic issues beyond dbt tests
- **Metric-level assertions**: compare historical aggregates (sum, count, median) and assert acceptable deltas.  
- **Grain assertions**: verify uniqueness of business-grain keys (using `dbt_utils.unique_combination_of_columns`).  
- **Row-level sampling & diff**: diff a small sample of pre/post-transformation rows to surface incorrect joins/duplication.  
- **Temporal sanity checks**: ensure `start_date ≤ end_date`, `service_date` within expected windows.  
- **Enrichment consistency**: check joins to enrichment tables do not multiply rows (one-to-many vs one-to-one mistakes).

---

### 4.5 Guardrails to prevent recurrence
- **Require partitioning & clustering standards** for large fact tables; enforce via PR review / CI checks.  
- **Contract tests for upstream feeds**: schema + cardinality expectations before acceptance.  
- **Pre-merge deduplication** macros to detect and drop duplicates before MERGE.  
- **Metric drift monitoring** (daily deltas, z-score alarms) with automated alerts.  
- **Enrichment & merge failure logging**: capture failed keys and retry queues.  
- **Cost/run-time alerts**: break jobs or notify when runtime or cost crosses thresholds.

---

### 4.6 First 30-minute triage plan
1. **Stop or throttle downstream schedules** to prevent further bad outputs.  
2. **Collect evidence**: run `dbt run` with `--models` on staging and the first intermediate models; save row counts and `run_results.json`.  
3. **Inspect warehouse query history** for the longest-running queries and check for full-table scans or broadcast joins.  
4. **Locate the expansion point** by comparing counts (raw → stg → int) to find where rows multiply.  
5. **Run a targeted `--full-refresh`** for the suspect model in an isolated environment to confirm whether incremental logic is the root cause.  
6. **If business-critical outputs are corrupted**, revert to last known-good snapshot and notify stakeholders with mitigation steps and ETA.  
7. **Document findings** and create short-term fixes (e.g., add pre-merge dedupe, tighten incremental predicate) and longer-term actions (partitioning, contract tests).


## sources for 4.
1. dbt Labs — Performance Best Practices

https://docs.getdbt.com/docs/best-practices/performance

-Covers:

Partitioning & clustering
Incremental model pitfalls
Join performance
Cost control 

2. dbt Labs — Debugging dbt Models

https://docs.getdbt.com/docs/guides/debugging

Covers:

Finding failing models
How to isolate regression
--full-refresh vs incremental
Large-model debugging techniques


3. dbt Incremental Models Guide

https://docs.getdbt.com/docs/build/incremental-models

Covers:

Incremental merge logic
Unique key problems
Row explosion issues
Bad incremental filters (is_incremental())
