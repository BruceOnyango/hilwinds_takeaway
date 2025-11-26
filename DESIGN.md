## 3. dbt System Design  

## Scope - what I understood from research

*(I have not used dbt professionally; this design reflects best practices gathered from research across dbt documentation, community sources, and ChatGPT-aided synthesis.)*  

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


### 3.2

- Incremental Merge Logic: Only process new or changed rows instead of rebuilding the whole table.(also seen in the water mark implementation in this repo)(use of unique keys to check then update if exists/ insert if otherwise)
- Schema & Column Tests: Automatic checks that your tables and columns look correct. ( not_null , no duplicates, and checks for values that should be unique such as person_id)
- Freshness Checks - Alerts when data stops updating.This could be through timestamps and background cron jobs
- Anomaly Checks - Spot weird patterns that tests may not catch. (e.g Claims suddenly jump from 100/day → 20,000/day)
- Metadata Columns - Extra columns used for auditing and debugging (e.g batch_id, is_deleted, updated_at etc)
- Safe Model Deprecation Strategy - Mark the model as “deprecated” -> Keep it in place but stop updating it -> Point dashboards to new models -> Point dashboards to new models -> Finally delete it

## 4.1 What probably went wrong
- The new feed added too many rows in the same partition, making queries slow.

- Or it introduced duplicates → joins suddenly explode.

- Or join keys changed → tables multiply unexpectedly.

- Or logic changed → the model still “passes tests” but produces wrong numbers.

## 4.2 How I would isolate the regression
I would compare the old inputs vs the new inputs to see what changed.

Specifically: 
- Row counts: Did the number of rows suddenly spike?

- Duplicates: Did the new feed introduce repeated IDs?

- Join keys: Did two tables now match on many more rows than expected?

- Partition distribution: Did all 20M rows land in the same date?

This helps pinpoint where the slowdown started.

## 4.3 How to detect issues that dbt tests miss

add tests like:

- Input vs output row count checks (did we lose or multiply rows?)

- Duplicate detection (did a new feed break uniqueness?)

- Referential integrity (are plan IDs valid?)

- Anomaly checks (did today’s row count jump 10×?)

## 4.4 How to prevent this from happening again

Guardrails:

- Partition pruning: ensure new data lands in the correct daily/monthly partition.

- Schema contracts: new feed must follow the agreed format.

- Row-count monitors: alert if volume changes too much.

- Cost alerts: notify if warehouse cost spikes.

- Uniqueness checks: catch duplicates early.

## 4.5 My First 30 Minutes

- Pull a tiny sample of the new feed to inspect it manually.

- Check for duplicates or missing IDs.

- Check the partition value (e.g., claim_date) to see if all data is stored under a single day.

- Run the slowest model on only 1% of data to see whether it still blows up.

## Edge Cases to Handle With More Time
1. For larger datasets process in batches (chunking)
2. duplicate column names check.
3. Retry loading the datasets with exponetional backoffs
4. checks for where the end date is earlier than the start date
