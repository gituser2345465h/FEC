# FEC Data Pipeline

Automated daily pull of FEC financial data for the 2026 election cycle, feeding the Domo FEC dashboard.

## Data Sources

| File | FEC Schedule | Description | API Endpoint |
|------|-------------|-------------|--------------|
| `data/FECScheduleE_latest.csv` | Schedule E | Independent Expenditures (IEs) by outside groups | `/v1/schedules/schedule_e/` |
| `data/FECScheduleB_latest.csv` | Schedule B | Disbursements from all committees | `/v1/schedules/schedule_b/` |

## How It Works

1. **GitHub Actions** runs `scripts/pull_schedule_e.py` and `scripts/pull_schedule_b.py` daily at 6:00 AM ET
2. Scripts pull from the [OpenFEC API](https://api.open.fec.gov/developers/) with cursor-based pagination
3. Flattened CSVs are committed to `data/`
4. **Domo GitHub connector** reads from the `data/` directory

## Setup

### 1. Add the FEC API key as a GitHub secret

Go to **Settings → Secrets and variables → Actions → New repository secret**

- Name: `FEC_API_KEY`
- Value: Your OpenFEC API key (get one at https://api.open.fec.gov/developers/)

### 2. Connect Domo

Point the Domo GitHub connector at:
- `data/FECScheduleE_latest.csv`
- `data/FECScheduleB_latest.csv`

### 3. Manual trigger

You can trigger a pull manually from the **Actions** tab → **FEC Data Pull** → **Run workflow**

## Changes from Original Pipeline

- **Removed `sort_null_only=true`** — was silently filtering out most records
- **Increased `per_page` from 20 to 100** — 5x fewer API calls, less risk of rate limit cutoff
- **Fixed stale date offset** in Schedule B (was hardcoded 36 days behind)
- **Added rate-limit sleep** (0.5s between requests) to avoid API throttling
- **Removed hardcoded AWS credentials** — no longer needed since S3 is eliminated
- **Automated daily schedule** — no longer depends on manual notebook execution
