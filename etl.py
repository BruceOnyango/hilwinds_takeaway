# etl.py
import pandas as pd
import json
import os
import re
import logging
import time
from random import random

OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    filename="logs/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

HWM_FILE = os.path.join(OUTPUT_DIR, "high_water_mark.json")


def run_etl():
    # -----------------------------
    # Load CSVs
    # -----------------------------
    employees = pd.read_csv("data/employees_raw.csv")
    plans = pd.read_csv("data/plans_raw.csv")
    claims = pd.read_csv("data/claims_raw.csv")

    # -----------------------------
    # Load company lookup & API mock
    # -----------------------------
    with open("company_lookup.json") as f:
        company_lookup = json.load(f)

    with open("api_mock.json") as f:
        api_mock = json.load(f)["sample_response"]

    # -----------------------------
    # High-water mark (incremental processing)
    # -----------------------------
    if os.path.exists(HWM_FILE):
        with open(HWM_FILE) as f:
            hwm = json.load(f)
    else:
        hwm = {"employees": None, "plans": None, "claims": None}

    # Filter only new rows for incremental run
    if hwm["employees"]:
        employees = employees[
            pd.to_datetime(employees["start_date"], errors="coerce")
            > pd.to_datetime(hwm["employees"])
        ]
    if hwm["plans"]:
        plans = plans[
            pd.to_datetime(plans["start_date"], errors="coerce")
            > pd.to_datetime(hwm["plans"])
        ]
    if hwm["claims"]:
        claims = claims[
            pd.to_datetime(claims["service_date"], errors="coerce")
            > pd.to_datetime(hwm["claims"])
        ]

    # -----------------------------
    # Early return if no new rows
    # -----------------------------
    if employees.empty and plans.empty and claims.empty:
        logging.info("No new rows to process. Exiting ETL early.")
        print("No new rows to process. ETL skipped.")
        return

    # -----------------------------
    # Helper functions
    # -----------------------------
    def safe_email(email):
        return bool(re.match(r"[^@]+@[^@]+\.[^@]+", str(email)))

    def infer_ein(email):
        if pd.isna(email) or "@" not in email:
            return None
        domain = email.split("@")[1]
        return company_lookup.get(domain)

    # -----------------------------
    # Validation and cleaning employees
    # -----------------------------
    if not employees.empty:
        employees["row_id"] = employees.index
        errors = []

        # Infer EINs if missing
        employees["company_ein"] = employees["company_ein"].fillna(
            employees["email"].apply(infer_ein)
        )

        # Validate emails
        for idx, email in employees["email"].items():
            if not safe_email(email):
                errors.append(
                    {"row_id": idx, "field": "email", "error_reason": "Invalid email"}
                )

        # Deduplicate
        employees = employees.drop_duplicates()

        # Validate dates
        for col in ["start_date"]:
            employees[col] = pd.to_datetime(employees[col], errors="coerce")
            for idx, val in employees[col].items():
                if pd.isna(val):
                    errors.append(
                        {"row_id": idx, "field": col, "error_reason": "Invalid date"}
                    )

        # Carry forward titles
        employees["title"] = employees.groupby("full_name")["title"].ffill().bfill()
    else:
        errors = []

    # -----------------------------
    # Plans and Claims cleaning
    # -----------------------------
    if not plans.empty:
        for col in ["start_date", "end_date"]:
            plans[col] = pd.to_datetime(plans[col], errors="coerce")

    if not claims.empty:
        claims["service_date"] = pd.to_datetime(claims["service_date"], errors="coerce")

    # -----------------------------
    # Enrichment (mock API)
    # -----------------------------
    enrichment_cache = {}

    def enrich_company(ein):
        if ein in enrichment_cache:
            logging.info(f"Cache hit for EIN {ein}")
            return enrichment_cache[ein]

        for attempt in range(1, 4):  # 3 attempts
            try:
                logging.info(f"Attempt {attempt} for enriching EIN {ein}")
                if random() < 0.2:  # simulate 20% API failure
                    raise Exception("Temporary API failure")
                enrichment_cache[ein] = api_mock
                logging.info(f"Enrichment successful for EIN {ein}")
                return api_mock
            except Exception as e:
                wait_time = 0.5 * attempt
                logging.warning(
                    f"Enrichment attempt {attempt} failed for EIN {ein}: {e}. Retrying in {wait_time}s"
                )
                time.sleep(wait_time)

        # After retries fail
        logging.error(
            f"Enrichment failed for EIN {ein} after 3 attempts. Using defaults."
        )
        enrichment_cache[ein] = {"industry": None, "revenue": None, "headcount": None}
        return enrichment_cache[ein]

    if not employees.empty:
        unique_eins = employees["company_ein"].dropna().unique()
        enrichment_df = pd.DataFrame(
            [{"company_ein": ein, **enrich_company(ein)} for ein in unique_eins]
        )

        # Merge enrichment
        employees = employees.merge(enrichment_df, on="company_ein", how="left")

    # -----------------------------
    # Write validation errors
    # -----------------------------
    pd.DataFrame(errors).to_csv(
        os.path.join(OUTPUT_DIR, "validation_errors.csv"), index=False
    )

    # -----------------------------
    # Write cleaned/merged Parquet
    # -----------------------------
    clean_file = os.path.join(OUTPUT_DIR, "clean_data.parquet")
    if not employees.empty:
        if os.path.exists(clean_file):
            old_df = pd.read_parquet(clean_file)
            employees = pd.concat([old_df, employees]).drop_duplicates()
        employees.to_parquet(clean_file, index=False)

    # -----------------------------
    # Update high-water mark
    # -----------------------------
    hwm["employees"] = (
        str(employees["start_date"].max()) if not employees.empty else hwm["employees"]
    )
    hwm["plans"] = str(plans["start_date"].max()) if not plans.empty else hwm["plans"]
    hwm["claims"] = (
        str(claims["service_date"].max()) if not claims.empty else hwm["claims"]
    )

    with open(HWM_FILE, "w") as f:
        json.dump(hwm, f)

    logging.info(f"Rows processed: {len(employees)}, Validation errors: {len(errors)}")
    print(f"Rows processed: {len(employees)}, Validation errors: {len(errors)}")
