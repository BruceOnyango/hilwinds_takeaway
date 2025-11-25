Hillwinds Data Engineer Take‑Home (1–2 Hours)
=============================================

Timing & Expectations
---------------------
- Target effort: 1–2 focused hours (aim for ~90 minutes). Submit within 72 hours.
- Work solo; online references are fine. Note assumptions and any shortcuts you take.
- Prioritize in order: SQL → Python ETL → dbt note → debugging note. If you run short, finish earlier items and briefly state what remains.
- What we care about: correct outputs, sensible edge handling, reproducible runs, clear written reasoning, and pragmatic scoping (what you shipped vs deferred).

Data Provided (repo root)
-------------------------
- `employees_raw.csv`, `plans_raw.csv`, `claims_raw.csv`
- `company_lookup.json` (email domain → EIN)
- `api_mock.json` (mock enrichment endpoint/shape)

Deliverables
------------
Place outputs in `outputs/`:
- SQL results: `sql_gaps.csv`, `sql_spikes.csv`, `sql_roster.csv`.
- Python artifacts: `validation_errors.csv`, `clean_data.parquet` (both in `outputs/`).
- `README.md`: setup, a single command (or two) to run everything, and assumptions/edge cases handled or deferred.
- `DESIGN.md`: dbt + debugging notes.
- Optional: `requirements.txt` and a couple of quick assertions/tests for key helpers.

Tasks
-----
**1) Advanced SQL (keep runnable; DuckDB or SQLite)**  
- **Plan Gap Detection:** Normalize overlapping plan intervals per `company` (EIN or name) + `plan_type`, stitch adjacent intervals, find gaps >7 days. Output `company_name, gap_start, gap_end, gap_length_days, previous_carrier, next_carrier` → `outputs/sql_gaps.csv`.  
- **Claims Cost Spike:** Rolling 90-day claim costs by company ordered by `service_date`; flag >200% spikes. Output `company_name, window_start, window_end, prev_90d_cost, current_90d_cost, pct_change` → `outputs/sql_spikes.csv`.  
- **Employee Roster Mismatch:** Compare eligibility data vs `employee_count` for companies; severity: Low (<20%), Medium (20–50%), High (50–100%), Critical (>100%). Output `company_name, expected, observed, pct_diff, severity` → `outputs/sql_roster.csv`. If `employee_count` is not present, define a simple expected mapping in-sql/README (e.g., `11-1111111=60, 22-2222222=45, 33-3333333=40`) and note the assumption.  
- If short on time, briefly note any edge cases you would handle with more time.

**2) Advanced Python ETL**  
- Ingest `employees_raw.csv`, `plans_raw.csv`, `claims_raw.csv`.  
- Clean: dedupe rows; coerce/validate dates; extract any structured fields from `notes`; infer missing EINs using `company_lookup.json`; drop clearly bad emails; carry forward titles where possible.  
- Enrich: use the mock endpoint shape in `api_mock.json`; simulate it locally with caching and simple retry/backoff (no external calls).  
- Validation framework: invalid rows → `outputs/validation_errors.csv` with `row_id, field, error_reason`; valid rows → `outputs/clean_data.parquet`. Log summary metrics (rows in/out, errors).  
- Incrementalism: track a high-water mark (SQLite or file) on `last_updated`/dates; reruns should only process new/updated rows but still emit merged outputs. If time is tight, stub it and describe what you’d harden next.  
- Keep it lean: prioritize correctness and clarity. Small assertions for key helpers are encouraged.

**3) dbt System Design (in `DESIGN.md`)**  
- Outline staging → intermediate → fact models for these sources + enrichment (5–7 bullets).  
- Describe incremental merge logic, schema/column tests, freshness/anomaly checks, metadata columns, and a safe model deprecation strategy.

**4) Deep Debugging & Incident Response (in `DESIGN.md`)**  
- Scenario: new feed adds 20M rows/day; costs triple; models slow 4m → 45m; outputs wrong despite passing dbt tests.  
- Briefly explain likely causes (partitioning/clustering, key explosion, duplicates), how you’d isolate the regression, detect logic issues beyond tests, guardrails to prevent recurrence, and your first 30-minute plan.

Evaluation
----------
- Data correctness and sensible edge handling.
- Reliability (validation, retries/caching, incremental thinking).
- Clarity of reasoning and communication (assumptions, trade-offs, what you’d do with more time).
- System/operational thinking (dbt design, incident approach).
- Reproducibility and polish: can we run it with the provided commands, and are logs/outputs easy to inspect?

Submission
----------
Send a link to your repo/zip with all deliverables. Include exact commands to set up and run locally. If anything is incomplete, note it explicitly so we can focus feedback on what you finished.
