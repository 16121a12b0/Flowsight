# ❄️ FlowSight — Openflow (NiFi) Job Status Dashboard

FlowSight is a **Streamlit-in-Snowflake** dashboard that gives data and platform
engineers a single, real-time view of **Apache NiFi (Openflow)** data-ingestion
job health across many runtime environments. Instead of reading raw event logs
in Snowflake to diagnose failures, operators get filterable job cards, failure
drill-downs, per-job history, hourly trends, and a cross-runtime overview.

> **Note:** This is a portfolio version of an internal tool. Organization-specific
> database, schema, warehouse, role, and runtime names have been genericized.
> Configure your own values in `RUNTIME_CONFIG` and the deployment settings.

---

## ✨ Features

- **Job Status** — latest status per process group (Running / Completed / Failed /
  Stopped) for the selected runtime, with search (wildcards like `*kafka*`),
  summary metrics, and a long-running-job alert banner.
- **Failure triage** — inline error messages and a full 24-hour event history per
  job, so failures can be diagnosed without leaving the dashboard.
- **Trends** — hourly stacked area chart of job counts by status.
- **All Runtimes overview** — per-runtime status counts plus aggregate failure /
  running alerts across every configured runtime.
- **Auto-refresh** — configurable (Off / 30s / 1m / 2m / 5m) with a live countdown,
  plus a manual "Refresh Now".
- **Theme-aware** — automatically follows the user's Snowsight light/dark theme.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────┐
│                 Snowflake Streamlit (FlowSight)            │
├──────────────────────────────────────────────────────────┤
│  Presentation (Streamlit)                                  │
│  ├── Sidebar: runtime, filters, sort, threshold, refresh   │
│  ├── Tab 1: Job Status (cards, summary, error drilldown)   │
│  ├── Tab 2: Trends (hourly stacked area chart)             │
│  └── Tab 3: All Runtimes overview table                    │
├──────────────────────────────────────────────────────────┤
│  Data access (Snowpark SQL, cached)                        │
│  ├── fetch_jobs()                                          │
│  ├── fetch_job_history()                                   │
│  ├── fetch_failed_job_errors()                             │
│  ├── fetch_hourly_trend()                                  │
│  └── fetch_multi_runtime_summary()                         │
├──────────────────────────────────────────────────────────┤
│  Source data (Snowflake event tables)                      │
│  └── <YOUR_DB>.<YOUR_SCHEMA>.OPENFLOW_EVENTS               │
└──────────────────────────────────────────────────────────┘
```

- The app runs **inside Snowflake** and obtains its Snowpark session via
  `get_active_session()` — there is no external connection string, and it inherits
  the executing user's role and privileges.
- All compute runs on the assigned warehouse. **No data leaves Snowflake.**
- FlowSight is **read-only** — it never starts, stops, or mutates jobs.

---

## 🔎 How It Works

Each status query follows the same shape:

1. **Window** — only events newer than 24h before the *latest* event
   (`TIMESTAMP > DATEADD(hour, -24, (SELECT MAX(TIMESTAMP) FROM <table>))`),
   relative to the newest event rather than wall-clock now.
2. **Extract** the job name from the event text via regex `name=([^\]\,]+)`.
3. **Classify** status with a `CASE` expression (keyword-based, see below).
4. **Deduplicate** with `ROW_NUMBER()` partitioned by job name, ordered by
   `TIMESTAMP DESC`, keeping only the most recent event per job (`rn = 1`).

Filters applied to every query: namespace matches the selected runtime,
`RECORD_TYPE = 'LOG'`, message contains `ProcessGroup`/`ProcessorNode`,
heartbeat/protocol noise excluded, and job name not null/empty. Values that reach
SQL (e.g. job name) are escaped for single quotes before interpolation.

### Status classification

| Status | Trigger keywords |
|---|---|
| Failed | `Failed to synchronize`, `ERROR` |
| Stopped | `removed from flow`, `stopped`, `Stopped` |
| Completed | `Successfully`, `checkpointed`, `completed` |
| Running | default (no other match) |

---

## ⚙️ Configuration

Each runtime maps to an event table and a Kubernetes namespace. Add a runtime by
adding an entry to `RUNTIME_CONFIG`:

```python
RUNTIME_CONFIG = {
    "example-runtime": {
        "event_table": "<YOUR_DB>.<YOUR_SCHEMA>.OPENFLOW_EVENTS",
        "namespace": "runtime-example-runtime",
    },
    # ... add more runtimes here
}
```

Tunable constants:

| Constant | Default | Meaning |
|---|---|---|
| `LONG_RUNNING_THRESHOLD_HOURS` | 6 | Running jobs older than this raise an alert (also adjustable in the sidebar) |
| `AUTO_REFRESH_OPTIONS` | Off / 30s / 1m / 2m / 5m | Auto-refresh intervals |
| `STATUS_COLORS` | — | Badge colours per status |

---

## 🧠 Caching

| Function | Cache TTL | Returns |
|---|---|---|
| `get_session()` | resource (persistent) | Snowpark session |
| `fetch_jobs()` | 60s | Latest status per job |
| `fetch_job_history()` | 60s | Up to 50 recent events for one job |
| `fetch_failed_job_errors()` | 60s | Up to 10 error rows for one job |
| `fetch_hourly_trend()` | 120s | Hourly count by status |
| `fetch_multi_runtime_summary()` | 300s | Per-runtime status counts |

Manual and auto-refresh both call `st.cache_data.clear()` then `st.rerun()`, so a
refresh always re-queries Snowflake.

---

## 🚀 Deployment

Deployed as a Snowflake Streamlit app. Set your own database, schema, stage, and
warehouse. Options:

1. **Snowsight UI** — edit in the Streamlit editor and save (applies immediately).
2. **SnowSQL CLI**
   ```bash
   snowsql -q "PUT file://./app.py @<YOUR_DB>.<YOUR_SCHEMA>.<YOUR_STAGE>/streamlit_app.py AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
   ```
3. **Stage upload via stored procedure** — load the code into a temp table and have
   a Python stored procedure write it to the stage (useful for repeatable deploys).

### Dependencies

`streamlit`, `pandas`, `snowflake-snowpark-python` — all provided by the Snowflake
Streamlit runtime; no external installs. Python runtime 3.11.

### Permissions

The executing role needs read access to the `OPENFLOW_EVENTS` table(s) and usage on
the warehouse. FlowSight performs no writes.

---

## ⚠️ Known Limitations

1. **Rolling 24h window** — only the last 24h relative to the newest event is shown.
2. **Keyword classification** — status can be misread when log phrasing is atypical.
3. **No historical persistence** — live view only; nothing stored for long-term SLA.
4. **Sequential multi-runtime queries** — the overview tab queries runtimes one at a
   time, so it slows as runtimes grow.
5. **In-Snowflake only** — relies on `get_active_session()`; running locally requires
   adapting the session bootstrap to build a Snowpark session from credentials.

---

## 🗺️ Roadmap

- Parallelise the All-Runtimes overview queries.
- Persist daily snapshots for real historical trends and SLAs.
- Configurable alerting (email / Slack) for failures and long-running jobs.
- User-selectable time window (6h / 24h / 7d).
- Structured status fields instead of keyword classification.

---

## 📂 Project Structure

```
flowsight/
├── app.py        # Streamlit application (local dev copy)
└── README.md     # This file
```

---

## 🛠️ Tech Stack

**Snowflake** · **Streamlit (Streamlit-in-Snowflake)** · **Snowpark** · **Python 3.11** · **SQL** · **pandas** · **Apache NiFi / Openflow**

---

## 👤 Author

**Sri Sushma Vardireddy** — Data Engineer

