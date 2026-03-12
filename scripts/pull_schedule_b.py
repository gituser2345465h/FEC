"""
FEC Schedule B (Disbursements) API Pull
Pulls all Schedule B filings for the current 2-year cycle from the OpenFEC API.
Outputs a flattened CSV to data/FECScheduleB_latest.csv

Fixes applied vs. original notebook:
- Removed sort_null_only=true (was filtering out most records)
- Increased per_page from 20 to 100
- Removed hardcoded date offset (was 36 days behind)
- Added rate-limit sleep between requests
- Dynamic cycle date range
- Secrets via environment variables

NOTE: Schedule B is MASSIVE — all disbursements from all committees.
The OpenFEC API caps results at ~500k records per query.
If you need candidate-specific disbursements, consider filtering by
committee_id for principal campaign committees only.
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
# Cap pages to avoid runaway pulls — Schedule B is enormous
MAX_PAGES = 5000
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "FECScheduleB_latest.csv")

BASE_URL = (
    f"https://api.open.fec.gov/v1/schedules/schedule_b/"
    f"?min_date={CYCLE_START}"
    f"&max_date={TODAY}"
    f"&per_page={PER_PAGE}"
    f"&sort=-disbursement_date"
    f"&sort_hide_null=true"
    f"&api_key={API_KEY}"
)


def fetch_all_pages():
    """Paginate through all Schedule B results."""
    all_results = []
    last_index = None
    last_disbursement_date = None
    page_count = 0

    while page_count < MAX_PAGES:
        if last_index and last_disbursement_date:
            url = f"{BASE_URL}&last_index={last_index}&last_disbursement_date={last_disbursement_date}"
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

        if page_count % 50 == 0:
            print(f"  Fetched {page_count} pages ({len(all_results)} records)...")

        # Get pagination cursors
        pagination = data.get("pagination", {})
        last_indexes = pagination.get("last_indexes")

        if not last_indexes:
            print(f"Pagination complete. Total pages: {page_count}")
            break

        last_index = last_indexes.get("last_index")
        last_disbursement_date = last_indexes.get("last_disbursement_date")

        if not last_index or not last_disbursement_date:
            print(f"Missing pagination cursor. Total pages: {page_count}")
            break

        time.sleep(RATE_LIMIT_SLEEP)

    if page_count >= MAX_PAGES:
        print(f"WARNING: Hit max page limit ({MAX_PAGES}). Data may be incomplete.")

    return all_results


def flatten_record(record):
    """Flatten a single Schedule B API record into a flat dict."""
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
        "candidate_office_state_full": record.get("candidate_office_state_full"),
        "category_code": record.get("category_code"),
        "category_code_full": record.get("category_code_full"),
        "comm_dt": record.get("comm_dt"),
        # Committee fields
        "committee_name": committee.get("name", ""),
        "committee_id": record.get("committee_id"),
        "committee_type": committee.get("committee_type", ""),
        "committee_designation": committee.get("designation", ""),
        "committee_designation_full": committee.get("designation_full", ""),
        "committee_party": committee.get("party", ""),
        "committee_party_full": committee.get("party_full", ""),
        "committee_state": committee.get("state", ""),
        "committee_city": committee.get("city", ""),
        "committee_cycle": committee.get("cycle", ""),
        "committee_is_active": committee.get("is_active", ""),
        "committee_organization_type": committee.get("organization_type", ""),
        "committee_organization_type_full": committee.get("organization_type_full", ""),
        "committee_treasurer_name": committee.get("treasurer_name", ""),
        "committee_affiliated_name": committee.get("affiliated_committee_name", ""),
        "committee_filing_frequency": committee.get("filing_frequency", ""),
        # Disbursement fields
        "disbursement_amount": record.get("disbursement_amount"),
        "disbursement_date": record.get("disbursement_date"),
        "disbursement_description": record.get("disbursement_description"),
        "disbursement_purpose_category": record.get("disbursement_purpose_category"),
        "disbursement_type": record.get("disbursement_type"),
        "disbursement_type_description": record.get("disbursement_type_description"),
        # Election fields
        "election_type": record.get("election_type"),
        "election_type_full": record.get("election_type_full"),
        "fec_election_type_desc": record.get("fec_election_type_desc"),
        "fec_election_year": record.get("fec_election_year"),
        # Entity fields
        "entity_type": record.get("entity_type"),
        "entity_type_desc": record.get("entity_type_desc"),
        # Payee/Recipient fields
        "payee_employer": record.get("payee_employer"),
        "payee_first_name": record.get("payee_first_name"),
        "payee_last_name": record.get("payee_last_name"),
        "payee_middle_name": record.get("payee_middle_name"),
        "payee_occupation": record.get("payee_occupation"),
        "recipient_city": record.get("recipient_city"),
        "recipient_committee_id": record.get("recipient_committee_id"),
        "recipient_name": record.get("recipient_name"),
        "recipient_state": record.get("recipient_state"),
        "recipient_zip": record.get("recipient_zip"),
        # Filing fields
        "file_number": record.get("file_number"),
        "filing_form": record.get("filing_form"),
        "image_number": record.get("image_number"),
        "line_number": record.get("line_number"),
        "line_number_label": record.get("line_number_label"),
        "link_id": record.get("link_id"),
        "load_date": record.get("load_date"),
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
        "two_year_transaction_period": record.get("two_year_transaction_period"),
        # Spender committee metadata
        "spender_committee_designation": record.get("spender_committee_designation"),
        "spender_committee_org_type": record.get("spender_committee_org_type"),
        "spender_committee_type": record.get("spender_committee_type"),
        # Conduit fields
        "conduit_committee_name": record.get("conduit_committee_name"),
        "conduit_committee_city": record.get("conduit_committee_city"),
        "conduit_committee_state": record.get("conduit_committee_state"),
    }


def main():
    print(f"Pulling FEC Schedule B data: {CYCLE_START} to {TODAY}")
    print(f"Per page: {PER_PAGE} | Max pages: {MAX_PAGES}")

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
    print(f"  Unique committees: {df['committee_id'].nunique()}")
    print(f"  Date range: {df['disbursement_date'].min()} to {df['disbursement_date'].max()}")
    print(f"  Total disbursement amount: ${df['disbursement_amount'].sum():,.2f}")


if __name__ == "__main__":
    main()
