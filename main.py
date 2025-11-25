import sqlite3
import csv
import os
import re
import logging
from etl import run_etl

DB = "data_engineering.db"
OUTPUT_DIR = "outputs"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------------------------
# Connect to SQLite
# -------------------------------------------
conn = sqlite3.connect(DB)
cur = conn.cursor()

# -------------------------------------------
# Helper: load a CSV into SQLite automatically
# -------------------------------------------
# -----------------------
# Configure logging to file only
# -----------------------
logging.basicConfig(
    filename="logs/csv_import.log",
    level=logging.INFO,
    filemode="a",  # append instead of overwrite
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def safe_identifier(name):
    """Allow only letters, numbers, and underscores to avoid SQL injection."""
    return re.sub(r"\W+", "_", name)


def infer_type(value):
    """Infer SQLite column type from a sample value."""
    if value is None or value == "":
        return "TEXT"

    # Try integer
    try:
        int(value)
        return "INTEGER"
    except:
        pass

    # Try float
    try:
        float(value)
        return "REAL"
    except:
        pass

    return "TEXT"


def load_csv_to_sqlite(csv_path, table_name, conn):
    """
    Import a CSV into SQLite with:
    - Auto type detection
    - Streaming (no large memory usage)
    - SQL injection-safe table/column names
    - Error handling + rollback
    - Logs written to a file
    """
    try:
        cur = conn.cursor()

        # Sanitize table name
        table_name = safe_identifier(table_name)

        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)

            # Read header row
            headers = next(reader)
            headers_clean = [safe_identifier(h) for h in headers]

            # Peek the first data row to detect types
            first_row = next(reader, None)

            if first_row is None:
                raise Exception("CSV file contains headers but no data rows.")

            # Infer column types using the first data row
            col_types = [infer_type(v) for v in first_row]

            # Build CREATE TABLE statement
            col_defs = ", ".join(
                [f'"{name}" {ctype}' for name, ctype in zip(headers_clean, col_types)]
            )

            logging.info("Dropping table if it exists.")
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")

            logging.info("Creating new table.")
            cur.execute(f"CREATE TABLE {table_name} ({col_defs})")

            # Prepare insert query
            placeholders = ", ".join(["?"] * len(headers))
            insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

            # Insert first row
            cur.execute(insert_sql, first_row)
            row_count = 1

            # Stream remaining rows without loading into memory
            for row in reader:
                if not any(row):  # skip empty rows
                    continue
                cur.execute(insert_sql, row)
                row_count += 1

        conn.commit()
        print("CSV imported successfully.")
        logging.info(f"Import finished. Total rows inserted: {row_count}")
        logging.info("CSV import completed successfully.")

    except Exception as e:
        print("Failed to import CSV:", e)
        logging.error(f"Failed to import csv with error {e}")

        conn.rollback()


# -------------------------------------------
# Load provided CSVs
# -------------------------------------------
load_csv_to_sqlite("data/plans_raw.csv", "plans", conn)
load_csv_to_sqlite("data/claims_raw.csv", "claims", conn)
load_csv_to_sqlite("data/employees_raw.csv", "employees", conn)

# -----------------------------------------
# Normalize date columns (with logging)
# -----------------------------------------

date_columns = ["start_date", "end_date"]  # removed service_date

for col in date_columns:
    try:
        cur.execute(f"UPDATE plans SET {col} = date({col})")
        logging.info(f"Normalized date column '{col}' in table 'plans'.")
    except Exception as e:
        logging.warning(f"Skipped normalizing '{col}' in 'plans': {e}")

conn.commit()
logging.info("Date normalization completed successfully.")

# -------------------------------------------
# 1. PLAN GAP DETECTION
# -------------------------------------------
sql_gaps = """
DROP TABLE IF EXISTS events;
CREATE TABLE events AS
SELECT company_ein, plan_type, carrier_name, start_date AS d, +1 AS marker
FROM plans
UNION ALL
SELECT company_ein, plan_type, carrier_name, date(end_date, '+1 day') AS d, -1 AS marker
FROM plans;

DROP TABLE IF EXISTS boundaries;
CREATE TABLE boundaries AS
SELECT company_ein, plan_type, d
FROM events
GROUP BY company_ein, plan_type, d;

DROP TABLE IF EXISTS atomic_segments;
CREATE TABLE atomic_segments AS
WITH nexts AS (
    SELECT
        company_ein,
        plan_type,
        d AS seg_start,
        LEAD(d) OVER (
            PARTITION BY company_ein, plan_type ORDER BY d
        ) AS seg_end
    FROM boundaries
)
SELECT
    company_ein,
    plan_type,
    seg_start,
    date(seg_end, '-1 day') AS seg_end
FROM nexts
WHERE seg_end IS NOT NULL;

DROP TABLE IF EXISTS labeled;
CREATE TABLE labeled AS
SELECT
    a.company_ein,
    a.plan_type,
    a.seg_start,
    a.seg_end,
    p.carrier_name
FROM atomic_segments a
JOIN plans p
  ON p.company_ein = a.company_ein
 AND p.plan_type = a.plan_type
 AND p.start_date <= a.seg_start
 AND p.end_date >= a.seg_end;

DROP TABLE IF EXISTS stitched;
CREATE TABLE stitched AS
WITH flags AS (
    SELECT
        *,
        CASE
            WHEN LAG(carrier_name) OVER (
                PARTITION BY company_ein, plan_type ORDER BY seg_start
            ) = carrier_name
            AND julianday(seg_start) =
                julianday(LAG(seg_end) OVER (
                    PARTITION BY company_ein, plan_type ORDER BY seg_start
                )) + 1
            THEN 0 ELSE 1 END AS new_group
    FROM labeled
),
groups AS (
    SELECT * ,
           SUM(new_group) OVER (
               PARTITION BY company_ein, plan_type ORDER BY seg_start
           ) AS grp
    FROM flags
)
SELECT
    company_ein,
    plan_type,
    carrier_name,
    MIN(seg_start) AS start_date,
    MAX(seg_end) AS end_date
FROM groups
GROUP BY company_ein, plan_type, carrier_name, grp;

DROP TABLE IF EXISTS sql_gaps;
CREATE TABLE sql_gaps AS
WITH ordered AS (
    SELECT
        *,
        LEAD(start_date) OVER (
            PARTITION BY company_ein, plan_type ORDER BY start_date
        ) AS next_start,
        LEAD(carrier_name) OVER (
            PARTITION BY company_ein, plan_type ORDER BY start_date
        ) AS next_carrier
    FROM stitched
)
SELECT
    company_ein,
    date(end_date, '+1 day') AS gap_start,
    date(next_start, '-1 day') AS gap_end,
    (
        julianday(date(next_start, '-1 day')) -
        julianday(date(end_date, '+1 day')) + 1
    ) AS gap_length_days,
    carrier_name AS previous_carrier,
    next_carrier
FROM ordered
WHERE next_start IS NOT NULL
  AND (
        julianday(date(next_start, '-1 day')) -
        julianday(date(end_date, '+1 day')) + 1
      ) > 7;
"""
cur.executescript(sql_gaps)

# Export CSV
rows = cur.execute("SELECT * FROM sql_gaps")
colnames = [desc[0] for desc in rows.description]

with open("outputs/sql_gaps.csv", "w", newline="") as f:
    writer = csv.writer(f)

    # write header
    writer.writerow(colnames)

    # write rows
    for row in rows:
        writer.writerow(row)

# -------------------------------------------
# 2. CLAIMS COST SPIKE DETECTION
# -------------------------------------------

sql_spikes = """
DROP TABLE IF EXISTS sql_spikes;

CREATE TABLE sql_spikes AS
WITH daily AS (
    SELECT
        company_ein,
        date(service_date) AS service_date,
        SUM(amount) AS daily_cost
    FROM claims
    GROUP BY company_ein, date(service_date)
),
windows AS (
    SELECT
        d1.company_ein,
        d1.service_date AS window_end,

        -- window start = 89 days before end → inclusive 90-day window
        date(d1.service_date, '-89 day') AS window_start,

        -- current 90-day sum
        (
            SELECT SUM(d2.daily_cost)
            FROM daily d2
            WHERE d2.company_ein = d1.company_ein
              AND d2.service_date BETWEEN date(d1.service_date, '-89 day') AND d1.service_date
        ) AS current_90d_cost,

        -- previous 90-day window (ends the day before)
        (
            SELECT SUM(d2.daily_cost)
            FROM daily d2
            WHERE d2.company_ein = d1.company_ein
              AND d2.service_date BETWEEN date(d1.service_date, '-179 day')
                                      AND date(d1.service_date, '-90 day')
        ) AS prev_90d_cost
    FROM daily d1
),
spikes AS (
    SELECT
        company_ein,
        window_start,
        window_end,
        prev_90d_cost,
        current_90d_cost,
        CASE
            WHEN prev_90d_cost > 0 THEN (current_90d_cost - prev_90d_cost) * 1.0 / prev_90d_cost
            ELSE NULL
        END AS pct_change
    FROM windows
)
SELECT *
FROM spikes
WHERE pct_change IS NOT NULL
  AND pct_change > 2.0  -- >200%
ORDER BY company_ein, window_end;
"""

cur.executescript(sql_spikes)

# Export CSV
rows = cur.execute("SELECT * FROM sql_spikes")
colnames = [desc[0] for desc in rows.description]

with open("outputs/sql_spikes.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(colnames)
    for row in rows:
        writer.writerow(row)

# -------------------------------------------
# 3. EMPLOYEE ROSTER MISMATCH
# -------------------------------------------

sql_roster = """
DROP TABLE IF EXISTS sql_roster;

CREATE TABLE sql_roster AS
WITH expected_counts AS (
    SELECT '11-1111111' AS company_ein, 60 AS expected UNION ALL
    SELECT '22-2222222', 45 UNION ALL
    SELECT '33-3333333', 40
),
observed_counts AS (
    SELECT
        company_ein,
        COUNT(*) AS observed
    FROM employees
    GROUP BY company_ein
)
SELECT
    ec.company_ein AS company_name,
    ec.expected,
    COALESCE(oc.observed, 0) AS observed,
    ROUND(ABS(COALESCE(oc.observed,0) - ec.expected) * 100.0 / ec.expected, 2) AS pct_diff,
    CASE
        WHEN ABS(COALESCE(oc.observed,0) - ec.expected) * 100.0 / ec.expected < 20 THEN 'Low'
        WHEN ABS(COALESCE(oc.observed,0) - ec.expected) * 100.0 / ec.expected BETWEEN 20 AND 49 THEN 'Medium'
        WHEN ABS(COALESCE(oc.observed,0) - ec.expected) * 100.0 / ec.expected BETWEEN 50 AND 100 THEN 'High'
        ELSE 'Critical'
    END AS severity
FROM expected_counts ec
LEFT JOIN observed_counts oc
  ON ec.company_ein = oc.company_ein
ORDER BY ec.company_ein;
"""

cur.executescript(sql_roster)

# Export CSV
rows = cur.execute("SELECT * FROM sql_roster")
colnames = [desc[0] for desc in rows.description]

with open("outputs/sql_roster.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(colnames)
    for row in rows:
        writer.writerow(row)


conn.close()

print("All SQL outputs successfully generated in outputs/")
logging.info(f"All SQL outputs successfully generated in outputs/")
if __name__ == "__main__":
    print("Starting ETL...")
    run_etl()
    print("ETL completed successfully!")
