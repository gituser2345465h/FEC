"""
FEC Schedule E (Independent Expenditures) API Pull
Pulls all Schedule E filings for the current 2-year cycle from the OpenFEC API.
Outputs a flattened CSV to data/FECScheduleE_latest.csv

Fixes applied vs. original notebook:
- Removed sort_null_only=true (was filtering out most records)
- Increased per_page from 20 to 100
- Added rate-limit sleep between requests
- Dynamic cycle date range
- Secrets via environment variables
"""

import pandas as pd
import requests
import json
import time
import os
import sys
from datetime import date, timedelta

# --- Configuration ---
API_KEY = os.environ.get("FEC_API_KEY")
if not API_KEY:
    print("ERROR: FEC_API_KEY environment variable not set.")
    sys.exit(1)

# 2026 cycle: Jan 1, 2025 through today
CYCLE_START = "2025-01-01"
TODAY = str(date.today())
PER_PAGE = 100
RATE_LIMIT_SLEEP = 0.5  # seconds between requests
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "FECScheduleE_latest.csv")

BASE_URL = (
    f"https://api.open.fec.gov/v1/schedules/schedule_e/"
    f"?min_date={CYCLE_START}"
    f"&max_date={TODAY}"
    f"&per_page={PER_PAGE}"
    f"&sort=-expenditure_date"
    f"&sort_hide_null=true"
    f"&api_key={API_KEY}"
)


def fetch_all_pages():
    """Paginate through all Schedule E results."""
    all_results = []
    last_index = None
    last_expenditure_date = None
    page_count = 0

    while True:
        if last_index and last_expenditure_date:
            url = f"{BASE_URL}&last_index={last_index}&last_expenditure_date={last_expenditure_date}"
        else:
            url = BASE_URL

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request error on page {page_count + 1}: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
            except Exception as e2:
                print(f"Retry failed: {e2}. Saving what we have.")
                break

        results = data.get("results", [])
        if not results:
            print(f"No more results. Total pages fetched: {page_count}")
            break

        all_results.extend(results)
        page_count += 1

        if page_count % 25 == 0:
            print(f"  Fetched {page_count} pages ({len(all_results)} records)...")

        # Get pagination cursors
        pagination = data.get("pagination", {})
        last_indexes = pagination.get("last_indexes")

        if not last_indexes:
            print(f"Pagination complete. Total pages: {page_count}")
            break

        last_index = last_indexes.get("last_index")
        last_expenditure_date = last_indexes.get("last_expenditure_date")

        if not last_index or not last_expenditure_date:
            print(f"Missing pagination cursor. Total pages: {page_count}")
            break

        time.sleep(RATE_LIMIT_SLEEP)

    return all_results


def flatten_record(record):
    """Flatten a single Schedule E API record into a flat dict."""
    committee = record.get("committee") or {}

    return {
        "amendment_indicator": record.get("amendment_indicator"),
        "amendment_indicator_desc": record.get("amendment_indicator_desc"),
        "beneficiary_committee_name": record.get("beneficiary_committee_name"),
        "candidate_first_name": record.get("candidate_first_name"),
        "candidate_id": record.get("candidate_id"),
        "candidate_last_name": record.get("candidate_last_name"),
        "candidate_middle_name": record.get("candidate_middle_name"),
        "candidate_name": record.get("candidate_name"),
        "candidate_office": record.get("candidate_office"),
        "candidate_office_description": record.get("candidate_office_description"),
        "candidate_office_district": record.get("candidate_office_district"),
        "candidate_office_state": record.get("candidate_office_state"),
        "candidate_party": record.get("candidate_party"),
        "category_code": record.get("category_code"),
        "category_code_full": record.get("category_code_full"),
        "comm_dt": record.get("comm_dt"),
        # Committee fields (using original column names for Domo compatibility)
        "name": committee.get("name", ""),
        "committee_id": record.get("committee_id"),
        "committee_type": committee.get("committee_type", ""),
        "designation": committee.get("designation", ""),
        "designation_full": committee.get("designation_full", ""),
        "party": committee.get("party", ""),
        "party_full": committee.get("party_full", ""),
        "state": committee.get("state", ""),
        "city": committee.get("city", ""),
        "cycle": committee.get("cycle", ""),
        "is_active": committee.get("is_active", ""),
        "organization_type": committee.get("organization_type", ""),
        "organization_type_full": committee.get("organization_type_full", ""),
        "treasurer_name": committee.get("treasurer_name", ""),
        "affiliated_committee_name": committee.get("affiliated_committee_name", ""),
        "filing_frequency": committee.get("filing_frequency", ""),
        # Expenditure fields
        "expenditure_amount": record.get("expenditure_amount"),
        "expenditure_date": record.get("expenditure_date"),
        "expenditure_description": record.get("expenditure_description"),
        "dissemination_date": record.get("dissemination_date"),
        "support_oppose_indicator": record.get("support_oppose_indicator"),
        # Election fields
        "election_type": record.get("election_type"),
        "election_type_full": record.get("election_type_full"),
        "fec_election_type_desc": record.get("fec_election_type_desc"),
        "fec_election_year": record.get("fec_election_year"),
        # Entity fields
        "entity_type": record.get("entity_type"),
        "entity_type_desc": record.get("entity_type_desc"),
        # Payee fields
        "payee_first_name": record.get("payee_first_name"),
        "payee_last_name": record.get("payee_last_name"),
        "payee_middle_name": record.get("payee_middle_name"),
        "payee_city": record.get("payee_city"),
        "payee_occupation": record.get("payee_occupation"),
        # Filing fields
        "file_number": record.get("file_number"),
        "filing_form": record.get("filing_form"),
        "image_number": record.get("image_number"),
        "line_number": record.get("line_number"),
        "link_id": record.get("link_id"),
        "memo_code": record.get("memo_code"),
        "memo_code_full": record.get("memo_code_full"),
        "memo_text": record.get("memo_text"),
        "memoed_subtotal": record.get("memoed_subtotal"),
        "original_sub_id": record.get("original_sub_id"),
        "pdf_url": record.get("pdf_url"),
        "report_type": record.get("report_type"),
        "report_year": record.get("report_year"),
        "schedule_type": record.get("schedule_type"),
        "schedule_type_full": record.get("schedule_type_full"),
        "sub_id": record.get("sub_id"),
        "transaction_id": record.get("transaction_id"),
        # Conduit fields
        "conduit_committee_id": record.get("conduit_committee_id"),
        "conduit_committee_name": record.get("conduit_committee_name"),
        "conduit_committee_city": record.get("conduit_committee_city"),
        "conduit_committee_state": record.get("conduit_committee_state"),
    }


def main():
    print(f"Pulling FEC Schedule E data: {CYCLE_START} to {TODAY}")
    print(f"Per page: {PER_PAGE}")

    all_results = fetch_all_pages()
    print(f"Total records fetched: {len(all_results)}")

    if not all_results:
        print("No data retrieved. Exiting.")
        sys.exit(1)

    # Flatten and create DataFrame
    flattened = [flatten_record(r) for r in all_results]
    df = pd.DataFrame(flattened)

    # Write to CSV
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    print(f"Saved {len(df)} records to {OUTPUT_PATH}")

    # Print summary stats
    print(f"\nSummary:")
    print(f"  Unique candidates: {df['candidate_name'].nunique()}")
    print(f"  Unique committees (spenders): {df['committee_id'].nunique()}")
    print(f"  Unique states: {df['candidate_office_state'].nunique()}")
    print(f"  Date range: {df['expenditure_date'].min()} to {df['expenditure_date'].max()}")
    print(f"  Total expenditure amount: ${df['expenditure_amount'].sum():,.2f}")


if __name__ == "__main__":
    main()
