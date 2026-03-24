# Apache Airflow Integration Guide

Orchestrate VitalFold Engine data population and DynamoDB sync from Apache Airflow.

## Overview

The pipeline has three sequential phases, each triggered by an HTTP POST and monitored by polling `GET /simulate/status`:

| Phase | Endpoint | Purpose |
|-------|----------|---------|
| 1. Static Populate | `POST /populate/static` | Seed reference data (patients, providers, clinics, insurance) |
| 2. Dynamic Populate | `POST /populate/dynamic` | Seed date-dependent data (appointments, visits, vitals) |
| 3. DynamoDB Sync | `POST /simulate/date-range` | Write Aurora visit data to DynamoDB |

All endpoints require a JWT bearer token. Obtain one via `POST /api/v1/auth/admin-login`.

---

## Base URL

```
VITALFOLD_BASE_URL = "https://<your-host>"
```

Set this as an Airflow Variable or Connection.

---

## Authentication

### `POST /api/v1/auth/admin-login`

**Request:**
```json
{
  "username": "admin",
  "password": "<ADMIN_PASSWORD env var>"
}
```

**Response (200):**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "user": {
    "id": "00000000-0000-0000-0000-000000000001",
    "email": "admin@admin.internal",
    "created_at": "2026-03-24T12:00:00Z"
  }
}
```

The `token` field is a JWT. Pass it as `Authorization: Bearer <token>` on all subsequent requests.

**Token lifetime:** Configured server-side via `JWT_EXPIRY_HOURS` (default: 24h). For long-running DAGs, re-authenticate at the start of each DAG run.

---

## Phase 1: Static Populate

Seeds reference data into Aurora DSQL. Run once — rejects with 409 if static data already exists.

### `POST /populate/static`

**Request (all fields optional):**
```json
{
  "plans_per_company": 3,
  "providers": 50,
  "patients": 50000
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `plans_per_company` | 3 | Insurance plans per company (7 companies) |
| `providers` | 50 | Number of providers |
| `patients` | 50000 | Number of patients |

**Response (202):**
```json
{
  "message": "Static populate started — poll GET /simulate/status for progress"
}
```

**Error responses:**
- `409` — Static data already exists (patients > 0). Reset first.
- `409` — A run is already in progress.

### Polling for completion

`GET /simulate/status` — look for `populate_progress`:

```json
{
  "running": true,
  "populate_progress": {
    "current_step": "Patients",
    "steps_done": 4,
    "total_steps": 8,
    "rows_written": 50210,
    "is_complete": false
  }
}
```

**Complete when:** `running == false` (the server holds `is_complete: true` for 3 seconds, then clears progress and sets `running` to `false`).

---

## Phase 2: Dynamic Populate

Seeds date-dependent data (clinic schedules, appointments, medical records, patient visits, patient vitals) for a specific date range. Can be called multiple times for non-overlapping ranges.

### `POST /populate/dynamic`

**Request:**
```json
{
  "start_date": "2026-04-01",
  "end_date": "2026-06-29",
  "appointments_per_day": 100,
  "records_per_appointment": 1
}
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `start_date` | Yes | — | Inclusive start date (YYYY-MM-DD) |
| `end_date` | Yes | — | Inclusive end date (YYYY-MM-DD) |
| `appointments_per_day` | No | 100 | Appointments generated per day |
| `records_per_appointment` | No | 1 | Medical records per appointment |

**Constraints:**
- Date range cannot exceed 90 days.
- Must not overlap with already-populated dates (returns 400 with conflict details).
- Requires static data from Phase 1 (returns 400 if patients == 0).

**Response (202):**
```json
{
  "message": "Dynamic populate started (2026-04-01 to 2026-06-29) — poll GET /simulate/status for progress"
}
```

### Polling

Same as Phase 1 — poll `GET /simulate/status` until `running == false`.

### Pre-check: already-populated dates

`GET /populate/dates` returns an array of date strings:

```json
["2026-04-01", "2026-04-02", "2026-04-03", ...]
```

Use this to avoid overlap errors when building date ranges dynamically.

---

## Phase 3: DynamoDB Sync

Reads patient visits + vitals from Aurora for a date range and writes them to both DynamoDB tables (`patient_visit`, `patient_vitals`). No Aurora data is generated.

### `POST /simulate/date-range`

**Request:**
```json
{
  "start_date": "2026-04-01",
  "end_date": "2026-06-29"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `start_date` | Yes | Inclusive start date (YYYY-MM-DD) |
| `end_date` | Yes | Inclusive end date (YYYY-MM-DD) |

**Constraints:**
- Date range cannot exceed 90 days.
- Returns 400 if no patient visits exist in Aurora for this range (run Phase 2 first).

**Response (202):**
```json
{
  "message": "DynamoDB sync started (2026-04-01 to 2026-06-29), syncing 9000 visits"
}
```

### Polling

`GET /simulate/status` — look for `dynamo_progress`:

```json
{
  "running": true,
  "dynamo_progress": {
    "operation": "Syncing to DynamoDB",
    "current_table": "Patient Visits",
    "tables_done": 0,
    "total_tables": 2,
    "items_processed": 3200,
    "total_items": 18000,
    "is_complete": false
  }
}
```

**Complete when:** `running == false`.

---

## Verification

### `GET /simulate/db-counts`

Returns exact row counts from both Aurora and DynamoDB:

```json
{
  "insurance_companies": 7,
  "insurance_plans": 21,
  "clinics": 10,
  "providers": 50,
  "patients": 50000,
  "emergency_contacts": 50000,
  "patient_demographics": 50000,
  "patient_insurance": 50000,
  "clinic_schedules": 900,
  "appointments": 9000,
  "medical_records": 9000,
  "patient_visits": 9000,
  "patient_vitals": 9000,
  "dynamo_patient_visits": 9000,
  "dynamo_patient_vitals": 9000
}
```

Use this to assert expected counts in your DAG.

---

## Reset Endpoints

| Endpoint | Scope | Async |
|----------|-------|-------|
| `POST /simulate/reset` | All Aurora DSQL data | Yes (poll `reset_progress`) |
| `POST /populate/reset-dynamic` | Dynamic data only (keeps patients, providers, etc.) | Yes (poll `reset_progress`) |
| `POST /simulate/reset-dynamo` | Both DynamoDB tables | Yes (poll `dynamo_progress`) |

All return 202 and run in the background. Poll `GET /simulate/status` for progress.

---

## Airflow DAG Example

```python
"""
VitalFold Engine — Full population DAG.

Phases:
  1. Static Populate  — reference data into Aurora DSQL
  2. Dynamic Populate — date-dependent data into Aurora DSQL
  3. DynamoDB Sync    — write Aurora visits to DynamoDB

Each phase is fire-and-poll: POST to start, then poll GET /simulate/status
until running == false.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import requests
import time

VITALFOLD_BASE_URL = Variable.get("vitalfold_base_url")
ADMIN_USERNAME = Variable.get("vitalfold_admin_username")
ADMIN_PASSWORD = Variable.get("vitalfold_admin_password", deserialize_json=False)

# How long to wait between status polls
POLL_INTERVAL_SECS = 5
# Maximum time to wait for any single phase
PHASE_TIMEOUT_SECS = 1800  # 30 minutes


def _get_token() -> str:
    """Authenticate and return a JWT token."""
    resp = requests.post(
        f"{VITALFOLD_BASE_URL}/api/v1/auth/admin-login",
        json={"username": ADMIN_USERNAME, "password": ADMIN_PASSWORD},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _poll_until_idle(token: str, timeout: int = PHASE_TIMEOUT_SECS) -> dict:
    """Poll GET /simulate/status until running == false. Returns final status."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = requests.get(
            f"{VITALFOLD_BASE_URL}/simulate/status",
            headers=_headers(token),
            timeout=10,
        )
        resp.raise_for_status()
        status = resp.json()
        if not status["running"]:
            return status
        time.sleep(POLL_INTERVAL_SECS)
    raise TimeoutError("Phase did not complete within timeout")


def _post_and_poll(token: str, path: str, body: dict | None = None) -> dict:
    """POST to start a phase, then poll until idle."""
    resp = requests.post(
        f"{VITALFOLD_BASE_URL}{path}",
        json=body,
        headers=_headers(token),
        timeout=30,
    )
    resp.raise_for_status()
    return _poll_until_idle(token)


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="vitalfold_populate",
    description="Populate VitalFold Aurora DSQL and sync to DynamoDB",
    schedule=None,  # trigger manually or via external sensor
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["vitalfold", "data-population"],
) as dag:

    @task()
    def authenticate() -> str:
        return _get_token()

    @task()
    def populate_static(token: str) -> dict:
        """Phase 1: Seed reference data (patients, providers, clinics, insurance)."""
        return _post_and_poll(token, "/populate/static", {
            "providers": 50,
            "patients": 50000,
            "plans_per_company": 3,
        })

    @task()
    def populate_dynamic(token: str) -> dict:
        """Phase 2: Seed date-dependent data (appointments, visits, vitals)."""
        start = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
        end = (datetime.utcnow() + timedelta(days=90)).strftime("%Y-%m-%d")
        return _post_and_poll(token, "/populate/dynamic", {
            "start_date": start,
            "end_date": end,
            "appointments_per_day": 100,
            "records_per_appointment": 1,
        })

    @task()
    def sync_dynamo(token: str) -> dict:
        """Phase 3: Write Aurora visit data to DynamoDB."""
        start = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
        end = (datetime.utcnow() + timedelta(days=90)).strftime("%Y-%m-%d")
        return _post_and_poll(token, "/simulate/date-range", {
            "start_date": start,
            "end_date": end,
        })

    @task()
    def verify_counts(token: str) -> dict:
        """Verify row counts from both Aurora and DynamoDB."""
        resp = requests.get(
            f"{VITALFOLD_BASE_URL}/simulate/db-counts",
            headers=_headers(token),
            timeout=30,
        )
        resp.raise_for_status()
        counts = resp.json()

        assert counts["patients"] > 0, "No patients found after populate"
        assert counts["appointments"] > 0, "No appointments found after populate"
        assert counts["dynamo_patient_visits"] > 0, "No DynamoDB visits after sync"
        assert counts["dynamo_patient_vitals"] > 0, "No DynamoDB vitals after sync"

        return counts

    # DAG dependency chain
    token = authenticate()
    static_done = populate_static(token)
    dynamic_done = populate_dynamic(token)
    sync_done = sync_dynamo(token)
    verified = verify_counts(token)

    static_done >> dynamic_done >> sync_done >> verified
```

---

## Airflow DAG: Daily Incremental Sync

For production use where Dynamic Populate runs weekly and DynamoDB sync runs daily:

```python
"""
VitalFold Engine — Daily DynamoDB sync.

Assumes static + dynamic data already exist in Aurora.
Syncs today's date range to DynamoDB.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import requests
import time

VITALFOLD_BASE_URL = Variable.get("vitalfold_base_url")
ADMIN_USERNAME = Variable.get("vitalfold_admin_username")
ADMIN_PASSWORD = Variable.get("vitalfold_admin_password", deserialize_json=False)
POLL_INTERVAL_SECS = 5
PHASE_TIMEOUT_SECS = 600  # 10 minutes for single-day sync


def _get_token() -> str:
    resp = requests.post(
        f"{VITALFOLD_BASE_URL}/api/v1/auth/admin-login",
        json={"username": ADMIN_USERNAME, "password": ADMIN_PASSWORD},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _poll_until_idle(token: str, timeout: int = PHASE_TIMEOUT_SECS) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = requests.get(
            f"{VITALFOLD_BASE_URL}/simulate/status",
            headers=_headers(token),
            timeout=10,
        )
        resp.raise_for_status()
        status = resp.json()
        if not status["running"]:
            return status
        time.sleep(POLL_INTERVAL_SECS)
    raise TimeoutError("Sync did not complete within timeout")


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="vitalfold_daily_sync",
    description="Sync today's Aurora visits to DynamoDB",
    schedule="0 6 * * *",  # 6 AM UTC daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["vitalfold", "dynamo-sync"],
) as dag:

    @task()
    def authenticate() -> str:
        return _get_token()

    @task()
    def check_visits_exist(token: str, **context) -> str:
        """Verify Aurora has visits for today before attempting sync."""
        today = context["ds"]  # Airflow execution date YYYY-MM-DD
        resp = requests.get(
            f"{VITALFOLD_BASE_URL}/populate/dates",
            headers=_headers(token),
            timeout=10,
        )
        resp.raise_for_status()
        populated = resp.json()

        if today not in populated:
            raise ValueError(
                f"No Aurora data for {today}. "
                "Run Dynamic Populate for this date range first."
            )
        return today

    @task()
    def sync_today(token: str, target_date: str) -> dict:
        """Sync a single day to DynamoDB."""
        resp = requests.post(
            f"{VITALFOLD_BASE_URL}/simulate/date-range",
            json={"start_date": target_date, "end_date": target_date},
            headers=_headers(token),
            timeout=30,
        )
        resp.raise_for_status()
        return _poll_until_idle(token)

    @task()
    def verify(token: str) -> dict:
        resp = requests.get(
            f"{VITALFOLD_BASE_URL}/simulate/db-counts",
            headers=_headers(token),
            timeout=30,
        )
        resp.raise_for_status()
        counts = resp.json()
        assert counts["dynamo_patient_visits"] > 0, "DynamoDB visits empty after sync"
        return counts

    token = authenticate()
    target = check_visits_exist(token)
    synced = sync_today(token, target)
    verified = verify(token)

    target >> synced >> verified
```

---

## Airflow Variables

Set these in the Airflow UI or CLI:

| Variable | Example | Description |
|----------|---------|-------------|
| `vitalfold_base_url` | `https://vitalfold.example.com` | Server base URL (no trailing slash) |
| `vitalfold_admin_username` | `admin` | Matches `ADMIN_USERNAME` env var |
| `vitalfold_admin_password` | `<secret>` | Matches `ADMIN_PASSWORD` env var |

Store `vitalfold_admin_password` as a **Connection** or use Airflow's secret backend for production deployments.

---

## Error Handling Reference

| HTTP Status | Meaning | DAG Action |
|-------------|---------|------------|
| `202` | Phase started successfully | Begin polling |
| `400` | Bad request (invalid dates, no visits, overlap) | Fail task, log message |
| `401` | JWT expired or invalid | Re-authenticate and retry |
| `409` | Another run is in progress | Wait and retry (Airflow retry handles this) |
| `500` | Server error | Fail task, alert |

---

## Timing Estimates

These vary by instance size and Aurora DSQL provisioning:

| Phase | 50K patients, 100 appts/day, 90 days | Notes |
|-------|---------------------------------------|-------|
| Static Populate | ~2–4 min | Bulk inserts in 2500-row chunks |
| Dynamic Populate | ~3–6 min | 9,000 appointments + visits + vitals |
| DynamoDB Sync | ~2–5 min | 40 concurrent writers, exponential backoff |
| Verify (db-counts) | ~5–15 sec | COUNT(*) on Aurora + Scan COUNT on DynamoDB |
