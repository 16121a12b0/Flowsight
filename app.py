"""FlowSight — Openflow Job Status Dashboard.

Tracks Openflow job status (process groups) with their names, paths,
and statuses for all runtimes. Status is derived from Snowflake
OPENFLOW_EVENTS logs.

Features:
- Job status monitoring with search & filters
- Job run history timeline with event messages
- Job duration tracking
- Failed job error details (latest 5 inline, full in View History)
- Auto-refresh capability
- Respects user's Snowsight theme (dark/light) automatically
- Trend charts & multi-runtime overview
"""

from __future__ import annotations

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session
import time

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RUNTIME_CONFIG: dict[str, dict] = {
    "digitalpoc": {
        "event_table": "OPENFLOW.OPENFLOW.OPENFLOW_EVENTS",
        "namespace": "runtime-digitalpoc",
    },
    "retailpoc": {
        "event_table": "OPENFLOW.OPENFLOW.OPENFLOW_EVENTS",
        "namespace": "runtime-retailpoc",
    },
    "digitaldataeng": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitaldataeng",
    },
    "digitaldataeng2": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitaldataeng2",
    },
    "digitalsportseng": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitalsportseng",
    },
    "digitalgamingeng": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitalgamingeng",
    },
    "digitalgamingcasinoeng": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitalgamingcasinoeng",
    },
    "digitalgamingpokereng": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitalgamingpokereng",
    },
    "digitalgamingbingoeng": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitalgamingbingoeng",
    },
    "digitalgamingaccounteng": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitalgamingaccounteng",
    },
    "digitalsportsbookmakingeng": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitalsportsbookmakingeng",
    },
    "digitalsportsbrzeng": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitalsportsbrzeng",
    },
    "digitalsportskafkaevents": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitalsportskafkaevents",
    },
    "digitalaffiliateseng": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitalaffiliateseng",
    },
    "digitalextinteng": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-digitalextinteng",
    },
    "retaildataeng": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-retaildataeng",
    },
    "retaildataeng2": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-retaildataeng2",
    },
    "retaildataeng3": {
        "event_table": "OPENFLOW_EDP.OPENFLOW_EDP.OPENFLOW_EVENTS",
        "namespace": "runtime-retaildataeng3",
    },
}

STATUS_COLORS: dict[str, str] = {
    "Running":   "#1E90FF",
    "Stopped":   "#6c757d",
    "Completed": "#28a745",
    "Failed":    "#dc3545",
}

# Alert threshold: flag Running jobs older than this many hours
LONG_RUNNING_THRESHOLD_HOURS = 6

# Auto-refresh intervals in seconds
AUTO_REFRESH_OPTIONS = {
    "Off": 0,
    "30 seconds": 30,
    "1 minute": 60,
    "2 minutes": 120,
    "5 minutes": 300,
}


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


@st.cache_resource
def get_session():
    return get_active_session()


# ---------------------------------------------------------------------------
# Data fetching — Job status from event logs
# ---------------------------------------------------------------------------


@st.cache_data(ttl=60)
def fetch_jobs(event_table: str, namespace: str) -> pd.DataFrame:
    """Fetch process group jobs with names, paths, and status."""
    session = get_session()
    query = f"""
    WITH raw_events AS (
        SELECT
            REGEXP_SUBSTR(VALUE::STRING, 'name=([^\\\\]\\\\,]+)', 1, 1, 'e') AS JOB_NAME,
            COALESCE(
                REGEXP_SUBSTR(VALUE::STRING, 'bucket\\s+([^"\\\\,]+)', 1, 1, 'ie'),
                REGEXP_SUBSTR(VALUE::STRING, 'in bucket\\s+([^"\\\\,]+)', 1, 1, 'ie'),
                REGEXP_SUBSTR(VALUE::STRING, 'group=([^\\\\]\\\\,]+)', 1, 1, 'e'),
                ''
            ) AS PARENT_GROUP,
            CASE
                WHEN VALUE::STRING LIKE '%Failed to synchronize%' OR VALUE::STRING LIKE '%ERROR%' THEN 'Failed'
                WHEN VALUE::STRING LIKE '%removed from flow%' OR VALUE::STRING LIKE '%stopped%' OR VALUE::STRING LIKE '%Stopped%' THEN 'Stopped'
                WHEN VALUE::STRING LIKE '%Successfully%' OR VALUE::STRING LIKE '%checkpointed%' OR VALUE::STRING LIKE '%completed%' THEN 'Completed'
                ELSE 'Running'
            END AS STATUS,
            TIMESTAMP,
            ROW_NUMBER() OVER (
                PARTITION BY REGEXP_SUBSTR(VALUE::STRING, 'name=([^\\\\]\\\\,]+)', 1, 1, 'e')
                ORDER BY TIMESTAMP DESC
            ) AS rn
        FROM {event_table}
        WHERE RESOURCE_ATTRIBUTES['k8s.namespace.name']::STRING = '{namespace}'
          AND RECORD_TYPE = 'LOG'
          AND (VALUE::STRING LIKE '%ProcessGroup%' OR VALUE::STRING LIKE '%ProcessorNode%')
          AND VALUE::STRING NOT LIKE '%heartbeat%'
          AND VALUE::STRING NOT LIKE '%Heartbeat%'
          AND VALUE::STRING NOT LIKE '%Protocol%'
          AND REGEXP_SUBSTR(VALUE::STRING, 'name=([^\\\\]\\\\,]+)', 1, 1, 'e') IS NOT NULL
          AND TIMESTAMP > DATEADD(hour, -24, (SELECT MAX(TIMESTAMP) FROM {event_table}))
    )
    SELECT
        JOB_NAME,
        COALESCE(NULLIF(PARENT_GROUP, ''), 'root') AS FOLDER_PATH,
        STATUS,
        TIMESTAMP AS LAST_SEEN
    FROM raw_events
    WHERE rn = 1
      AND JOB_NAME != ''
    ORDER BY STATUS, JOB_NAME
    """
    try:
        df = session.sql(query).to_pandas()
        return df
    except Exception as exc:
        st.error(f"Query failed: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def fetch_job_history(event_table: str, namespace: str, job_name: str) -> pd.DataFrame:
    """Fetch status change history for a specific job (last 24h)."""
    session = get_session()
    # Escape single quotes in job_name
    safe_name = job_name.replace("'", "''")
    query = f"""
    SELECT
        REGEXP_SUBSTR(VALUE::STRING, 'name=([^\\\\]\\\\,]+)', 1, 1, 'e') AS JOB_NAME,
        CASE
            WHEN VALUE::STRING LIKE '%Failed to synchronize%' OR VALUE::STRING LIKE '%ERROR%' THEN 'Failed'
            WHEN VALUE::STRING LIKE '%removed from flow%' OR VALUE::STRING LIKE '%stopped%' OR VALUE::STRING LIKE '%Stopped%' THEN 'Stopped'
            WHEN VALUE::STRING LIKE '%Successfully%' OR VALUE::STRING LIKE '%checkpointed%' OR VALUE::STRING LIKE '%completed%' THEN 'Completed'
            ELSE 'Running'
        END AS STATUS,
        TIMESTAMP,
        LEFT(VALUE::STRING, 500) AS EVENT_MESSAGE
    FROM {event_table}
    WHERE RESOURCE_ATTRIBUTES['k8s.namespace.name']::STRING = '{namespace}'
      AND RECORD_TYPE = 'LOG'
      AND (VALUE::STRING LIKE '%ProcessGroup%' OR VALUE::STRING LIKE '%ProcessorNode%')
      AND VALUE::STRING NOT LIKE '%heartbeat%'
      AND VALUE::STRING NOT LIKE '%Heartbeat%'
      AND VALUE::STRING NOT LIKE '%Protocol%'
      AND REGEXP_SUBSTR(VALUE::STRING, 'name=([^\\\\]\\\\,]+)', 1, 1, 'e') = '{safe_name}'
      AND TIMESTAMP > DATEADD(hour, -24, (SELECT MAX(TIMESTAMP) FROM {event_table}))
    ORDER BY TIMESTAMP DESC
    LIMIT 50
    """
    try:
        df = session.sql(query).to_pandas()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def fetch_failed_job_errors(event_table: str, namespace: str, job_name: str) -> pd.DataFrame:
    """Fetch error details for a failed job."""
    session = get_session()
    safe_name = job_name.replace("'", "''")
    query = f"""
    SELECT
        TIMESTAMP,
        LEFT(VALUE::STRING, 1000) AS ERROR_MESSAGE
    FROM {event_table}
    WHERE RESOURCE_ATTRIBUTES['k8s.namespace.name']::STRING = '{namespace}'
      AND RECORD_TYPE = 'LOG'
      AND (VALUE::STRING LIKE '%ProcessGroup%' OR VALUE::STRING LIKE '%ProcessorNode%')
      AND REGEXP_SUBSTR(VALUE::STRING, 'name=([^\\\\]\\\\,]+)', 1, 1, 'e') = '{safe_name}'
      AND (VALUE::STRING LIKE '%Failed to synchronize%' OR VALUE::STRING LIKE '%ERROR%')
      AND TIMESTAMP > DATEADD(hour, -24, (SELECT MAX(TIMESTAMP) FROM {event_table}))
    ORDER BY TIMESTAMP DESC
    LIMIT 10
    """
    try:
        df = session.sql(query).to_pandas()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=120)
def fetch_hourly_trend(event_table: str, namespace: str) -> pd.DataFrame:
    """Fetch hourly status counts for the last 24 hours (trend chart)."""
    session = get_session()
    query = f"""
    WITH raw_events AS (
        SELECT
            REGEXP_SUBSTR(VALUE::STRING, 'name=([^\\\\]\\\\,]+)', 1, 1, 'e') AS JOB_NAME,
            CASE
                WHEN VALUE::STRING LIKE '%Failed to synchronize%' OR VALUE::STRING LIKE '%ERROR%' THEN 'Failed'
                WHEN VALUE::STRING LIKE '%removed from flow%' OR VALUE::STRING LIKE '%stopped%' OR VALUE::STRING LIKE '%Stopped%' THEN 'Stopped'
                WHEN VALUE::STRING LIKE '%Successfully%' OR VALUE::STRING LIKE '%checkpointed%' OR VALUE::STRING LIKE '%completed%' THEN 'Completed'
                ELSE 'Running'
            END AS STATUS,
            DATE_TRUNC('hour', TIMESTAMP) AS HOUR_BUCKET
        FROM {event_table}
        WHERE RESOURCE_ATTRIBUTES['k8s.namespace.name']::STRING = '{namespace}'
          AND RECORD_TYPE = 'LOG'
          AND (VALUE::STRING LIKE '%ProcessGroup%' OR VALUE::STRING LIKE '%ProcessorNode%')
          AND VALUE::STRING NOT LIKE '%heartbeat%'
          AND VALUE::STRING NOT LIKE '%Heartbeat%'
          AND VALUE::STRING NOT LIKE '%Protocol%'
          AND REGEXP_SUBSTR(VALUE::STRING, 'name=([^\\\\]\\\\,]+)', 1, 1, 'e') IS NOT NULL
          AND TIMESTAMP > DATEADD(hour, -24, (SELECT MAX(TIMESTAMP) FROM {event_table}))
    )
    SELECT
        HOUR_BUCKET,
        STATUS,
        COUNT(DISTINCT JOB_NAME) AS JOB_COUNT
    FROM raw_events
    GROUP BY HOUR_BUCKET, STATUS
    ORDER BY HOUR_BUCKET
    """
    try:
        df = session.sql(query).to_pandas()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_multi_runtime_summary() -> pd.DataFrame:
    """Fetch job counts per status for all runtimes (overview table)."""
    session = get_session()
    results = []
    for runtime_name, config in RUNTIME_CONFIG.items():
        event_table = config["event_table"]
        namespace = config["namespace"]
        query = f"""
        WITH raw_events AS (
            SELECT
                REGEXP_SUBSTR(VALUE::STRING, 'name=([^\\\\]\\\\,]+)', 1, 1, 'e') AS JOB_NAME,
                CASE
                    WHEN VALUE::STRING LIKE '%Failed to synchronize%' OR VALUE::STRING LIKE '%ERROR%' THEN 'Failed'
                    WHEN VALUE::STRING LIKE '%removed from flow%' OR VALUE::STRING LIKE '%stopped%' OR VALUE::STRING LIKE '%Stopped%' THEN 'Stopped'
                    WHEN VALUE::STRING LIKE '%Successfully%' OR VALUE::STRING LIKE '%checkpointed%' OR VALUE::STRING LIKE '%completed%' THEN 'Completed'
                    ELSE 'Running'
                END AS STATUS,
                ROW_NUMBER() OVER (
                    PARTITION BY REGEXP_SUBSTR(VALUE::STRING, 'name=([^\\\\]\\\\,]+)', 1, 1, 'e')
                    ORDER BY TIMESTAMP DESC
                ) AS rn
            FROM {event_table}
            WHERE RESOURCE_ATTRIBUTES['k8s.namespace.name']::STRING = '{namespace}'
              AND RECORD_TYPE = 'LOG'
              AND (VALUE::STRING LIKE '%ProcessGroup%' OR VALUE::STRING LIKE '%ProcessorNode%')
              AND VALUE::STRING NOT LIKE '%heartbeat%'
              AND VALUE::STRING NOT LIKE '%Heartbeat%'
              AND VALUE::STRING NOT LIKE '%Protocol%'
              AND REGEXP_SUBSTR(VALUE::STRING, 'name=([^\\\\]\\\\,]+)', 1, 1, 'e') IS NOT NULL
              AND TIMESTAMP > DATEADD(hour, -24, (SELECT MAX(TIMESTAMP) FROM {event_table}))
        )
        SELECT STATUS, COUNT(*) AS CNT
        FROM raw_events
        WHERE rn = 1 AND JOB_NAME != ''
        GROUP BY STATUS
        """
        try:
            df = session.sql(query).to_pandas()
            row = {"Runtime": runtime_name}
            for _, r in df.iterrows():
                row[r["STATUS"]] = int(r["CNT"])
            row.setdefault("Running", 0)
            row.setdefault("Completed", 0)
            row.setdefault("Failed", 0)
            row.setdefault("Stopped", 0)
            row["Total"] = row["Running"] + row["Completed"] + row["Failed"] + row["Stopped"]
            results.append(row)
        except Exception:
            results.append({
                "Runtime": runtime_name, "Running": 0, "Completed": 0,
                "Failed": 0, "Stopped": 0, "Total": 0
            })
    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# UI Components
# ---------------------------------------------------------------------------


def status_badge(status: str) -> str:
    color = STATUS_COLORS.get(status, "#6c757d")
    return (
        f'<span style="background-color:{color};color:white;'
        f'padding:2px 10px;border-radius:12px;font-size:0.8rem;'
        f'font-weight:600;">{status}</span>'
    )


def render_job_card(row, event_table: str, namespace: str) -> None:
    """Render a single job card with history and error details."""
    status = row["STATUS"]
    name = row["JOB_NAME"]
    path = row["FOLDER_PATH"] if row["FOLDER_PATH"] else "/"
    last_seen = str(row["LAST_SEEN"])[:19]

    icon = {"Running": "🔵", "Failed": "❌", "Stopped": "⏹", "Completed": "✅"}.get(status, "⚪")

    # Check for long-running alert
    alert = ""
    hours_ago = 0
    if status == "Running":
        try:
            seen_time = pd.to_datetime(row["LAST_SEEN"])
            hours_ago = (datetime.now() - seen_time).total_seconds() / 3600
            if hours_ago >= LONG_RUNNING_THRESHOLD_HOURS:
                alert = f" ⚠️ Running for {hours_ago:.1f}h"
        except Exception:
            pass

    with st.expander(f"{icon} {name}{alert}", expanded=False):
        # Display path
        path_display = path if path and path != "root" else "/ (root)"
        st.markdown(
            f"""<table style="border:none;border-collapse:collapse;">
<tr style="border:none;"><td style="border:none;padding:4px 12px 4px 0;font-weight:bold;">Job Name</td><td style="border:none;padding:4px 0;"><code>{name}</code></td></tr>
<tr style="border:none;"><td style="border:none;padding:4px 12px 4px 0;font-weight:bold;">Path</td><td style="border:none;padding:4px 0;">📂 {path_display}</td></tr>
<tr style="border:none;"><td style="border:none;padding:4px 12px 4px 0;font-weight:bold;">Status</td><td style="border:none;padding:4px 0;">{status_badge(status)}</td></tr>
<tr style="border:none;"><td style="border:none;padding:4px 12px 4px 0;font-weight:bold;">Last Seen</td><td style="border:none;padding:4px 0;">{last_seen}</td></tr>
</table>""",
            unsafe_allow_html=True,
        )

        if alert:
            st.warning(f"⚠️ This job has been running for an unusually long time ({hours_ago:.1f} hours).")

        # ── Feature #4: Failed Job Error Details ──────────────────────────────
        if status == "Failed":
            st.markdown("**🔴 Error Details (latest 5):**")
            df_errors = fetch_failed_job_errors(event_table, namespace, name)
            if df_errors.empty:
                st.info("No error details available.")
            else:
                for _, err_row in df_errors.head(5).iterrows():
                    err_time = str(err_row["TIMESTAMP"])[:19]
                    err_msg = str(err_row["ERROR_MESSAGE"])[:500]
                    st.markdown(f"**`{err_time}`**")
                    st.code(err_msg, language="text")

        # ── Feature #1: Job Run History (full history behind button) ──────────
        safe_key = name.replace(" ", "_").replace(".", "_")[:30]
        if st.button(f"📜 View History", key=f"hist_{safe_key}_{status}"):
            st.session_state[f"show_hist_{name}"] = not st.session_state.get(f"show_hist_{name}", False)

        if st.session_state.get(f"show_hist_{name}", False):
            st.markdown("**📜 Full Job History (last 24h):**")
            df_hist = fetch_job_history(event_table, namespace, name)
            if df_hist.empty:
                st.info("No history available.")
            else:
                for _, h_row in df_hist.iterrows():
                    h_time = str(h_row["TIMESTAMP"])[:19]
                    h_status = h_row["STATUS"]
                    h_msg = str(h_row["EVENT_MESSAGE"])[:500] if "EVENT_MESSAGE" in h_row.index else ""
                    h_icon = {"Running": "🔵", "Failed": "❌", "Stopped": "⏹", "Completed": "✅"}.get(h_status, "⚪")
                    st.markdown(f"{h_icon} **{h_time}** — {status_badge(h_status)}", unsafe_allow_html=True)
                    if h_msg:
                        st.code(h_msg, language="text")


def render_trend_chart(event_table: str, namespace: str) -> None:
    """Render hourly trend chart for the selected runtime."""
    df_trend = fetch_hourly_trend(event_table, namespace)
    if df_trend.empty:
        st.info("No trend data available.")
        return

    # Pivot for stacked area chart
    pivot = df_trend.pivot_table(
        index="HOUR_BUCKET", columns="STATUS", values="JOB_COUNT", fill_value=0
    ).reset_index()
    pivot = pivot.rename(columns={"HOUR_BUCKET": "Hour"})

    st.area_chart(pivot.set_index("Hour"))


def render_multi_runtime_overview() -> None:
    """Render multi-runtime comparison table."""
    with st.spinner("Loading all runtimes..."):
        df_summary = fetch_multi_runtime_summary()

    if df_summary.empty:
        st.warning("No data available.")
        return

    # Display the summary table
    display_df = df_summary[["Runtime", "Total", "Running", "Completed", "Failed", "Stopped"]]
    display_df = display_df.reset_index(drop=True)
    st.table(display_df)

    # Summary bar
    total_failed = df_summary["Failed"].sum()
    total_running = df_summary["Running"].sum()
    if total_failed > 0:
        st.error(f"🚨 {total_failed} failed job(s) across all runtimes")
    else:
        st.success("✅ No failures across any runtime")
    st.info(f"🔵 {total_running} job(s) currently running across all runtimes")




# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    st.set_page_config(page_title="Sushma - FlowSight", page_icon="❄️", layout="wide")

    st.title("❄️ FlowSight")
    st.caption("Openflow Job Status Dashboard")
    st.markdown("---")

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_jobs, tab_trends, tab_overview = st.tabs([
        "📋 Job Status", "📈 Trends", "🌐 All Runtimes"
    ])

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Filters")

        # Runtime selector
        st.markdown("**Select Runtime:**")
        selected_runtime = st.selectbox(
            "Runtime",
            options=list(RUNTIME_CONFIG.keys()),
            index=0,
            key="runtime_select",
        )

        st.markdown("---")

        # Job status filter
        st.markdown("**Job Status:**")
        show_running   = st.checkbox("🔵 Running",   value=True, key="chk_running")
        show_completed = st.checkbox("✅ Completed", value=True, key="chk_completed")
        show_stopped   = st.checkbox("⏹ Stopped",   value=False, key="chk_stopped")
        show_failed    = st.checkbox("❌ Failed",    value=True, key="chk_failed")

        st.markdown("---")

        # Sorting options
        st.markdown("**Sort By:**")
        sort_option = st.selectbox(
            "Sort",
            options=["Status", "Job Name (A-Z)", "Job Name (Z-A)", "Last Seen (Newest)", "Last Seen (Oldest)"],
            index=0,
            key="sort_select",
            label_visibility="collapsed",
        )

        st.markdown("---")

        # Long-running threshold
        st.markdown("**Alert Threshold (hours):**")
        threshold = st.number_input(
            "Hours", min_value=1, max_value=24, value=LONG_RUNNING_THRESHOLD_HOURS,
            key="threshold_input", label_visibility="collapsed",
        )

        st.markdown("---")

        # ── Feature #3: Auto-Refresh ─────────────────────────────────────────
        st.markdown("**🔄 Auto-Refresh:**")
        auto_refresh = st.selectbox(
            "Auto-Refresh Interval",
            options=list(AUTO_REFRESH_OPTIONS.keys()),
            index=0,
            key="auto_refresh_select",
            label_visibility="collapsed",
        )
        refresh_seconds = AUTO_REFRESH_OPTIONS[auto_refresh]

        if refresh_seconds > 0:
            st.success(f"Auto-refreshing every {auto_refresh}")
        else:
            st.caption("Auto-refresh disabled")

        st.markdown("---")

        if st.button("🔄 Refresh Now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")
        st.caption(f"Runtime: `{selected_runtime}`")

    # ── Auto-Refresh Logic ────────────────────────────────────────────────────
    if refresh_seconds > 0:
        # Track last refresh time
        if "last_auto_refresh" not in st.session_state:
            st.session_state["last_auto_refresh"] = time.time()

        elapsed = time.time() - st.session_state["last_auto_refresh"]
        if elapsed >= refresh_seconds:
            st.session_state["last_auto_refresh"] = time.time()
            st.cache_data.clear()
            st.rerun()

        # Show countdown
        remaining = int(refresh_seconds - elapsed)
        st.sidebar.caption(f"⏳ Next refresh in {remaining}s")

    # ── Fetch jobs ────────────────────────────────────────────────────────────
    config = RUNTIME_CONFIG[selected_runtime]
    event_table = config["event_table"]
    namespace = config["namespace"]

    # ── Tab 1: Job Status ─────────────────────────────────────────────────────
    with tab_jobs:
        with st.spinner(f"Loading jobs for {selected_runtime}..."):
            df_jobs = fetch_jobs(event_table, namespace)

        # Last refresh timestamp
        refresh_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.caption(f"🕒 Last refreshed: {refresh_time}")

        if df_jobs.empty:
            st.warning("No process groups found for this runtime in the last 24 hours.")
        else:
            # Apply filters
            wanted = []
            if show_running:   wanted.append("Running")
            if show_completed: wanted.append("Completed")
            if show_stopped:   wanted.append("Stopped")
            if show_failed:    wanted.append("Failed")

            if not wanted:
                st.info("Select at least one status filter.")
            else:
                df_filtered = df_jobs[df_jobs["STATUS"].isin(wanted)]

                # Search
                search = st.text_input(
                    "🔍 Search by job name or path (supports wildcards: *kafka*)",
                    key="search_input",
                )
                if search:
                    term = search.lower().replace("*", ".*")
                    df_filtered = df_filtered[
                        df_filtered["JOB_NAME"].str.lower().str.contains(term, na=False, regex=True)
                        | df_filtered["FOLDER_PATH"].str.lower().str.contains(term, na=False, regex=True)
                    ]

                # Apply sorting
                if sort_option == "Job Name (A-Z)":
                    df_filtered = df_filtered.sort_values("JOB_NAME", ascending=True)
                elif sort_option == "Job Name (Z-A)":
                    df_filtered = df_filtered.sort_values("JOB_NAME", ascending=False)
                elif sort_option == "Last Seen (Newest)":
                    df_filtered = df_filtered.sort_values("LAST_SEEN", ascending=False)
                elif sort_option == "Last Seen (Oldest)":
                    df_filtered = df_filtered.sort_values("LAST_SEEN", ascending=True)

                # Summary metrics
                counts = df_filtered["STATUS"].value_counts().to_dict()
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Total", len(df_filtered))
                c2.metric("🔵 Running",   counts.get("Running", 0))
                c3.metric("✅ Completed", counts.get("Completed", 0))
                c4.metric("❌ Failed",    counts.get("Failed", 0))
                c5.metric("⏹ Stopped",   counts.get("Stopped", 0))

                # Long-running alerts banner
                running_jobs = df_filtered[df_filtered["STATUS"] == "Running"].copy()
                if not running_jobs.empty:
                    running_jobs["LAST_SEEN_DT"] = pd.to_datetime(running_jobs["LAST_SEEN"], errors="coerce")
                    cutoff = datetime.now() - timedelta(hours=threshold)
                    long_running = running_jobs[running_jobs["LAST_SEEN_DT"] < cutoff]
                    if not long_running.empty:
                        st.warning(
                            f"⚠️ {len(long_running)} job(s) have been running for more than "
                            f"{threshold} hours: {', '.join(long_running['JOB_NAME'].tolist()[:5])}"
                            + ("..." if len(long_running) > 5 else "")
                        )

                st.markdown("---")

                # Job count & Summary button
                col_info, col_summary = st.columns([3, 1])
                with col_info:
                    st.markdown(f"**{len(df_filtered)} job(s) found**")
                with col_summary:
                    if st.button("📊 Summary", use_container_width=True, key="summary_btn"):
                        st.session_state["show_summary"] = not st.session_state.get("show_summary", False)

                # Show runtime summary of all jobs when requested
                if st.session_state.get("show_summary"):
                    st.markdown("#### 📊 Runtime Summary — All Jobs")
                    summary_df = df_filtered.groupby("STATUS").agg(
                        Job_Count=("JOB_NAME", "count"),
                        Last_Activity=("LAST_SEEN", "max"),
                    ).reset_index()
                    summary_df = summary_df.rename(columns={"STATUS": "Status", "Job_Count": "Job Count", "Last_Activity": "Last Activity"})
                    st.table(summary_df)

                    # Per-folder breakdown
                    folder_summary = df_filtered.groupby(["FOLDER_PATH", "STATUS"]).agg(
                        Job_Count=("JOB_NAME", "count"),
                    ).reset_index()
                    folder_summary = folder_summary.rename(columns={"FOLDER_PATH": "Folder", "STATUS": "Status", "Job_Count": "Job Count"})
                    folder_pivot = folder_summary.pivot_table(
                        index="Folder", columns="Status", values="Job Count", fill_value=0
                    ).reset_index()
                    st.markdown("**By Folder Path:**")
                    st.dataframe(folder_pivot, use_container_width=True)

                # Job cards grouped by status
                status_order = ["Failed", "Running", "Completed", "Stopped"]
                for status in status_order:
                    group = df_filtered[df_filtered["STATUS"] == status]
                    if group.empty:
                        continue
                    icon = {"Running": "🔵", "Failed": "❌", "Stopped": "⏹", "Completed": "✅"}.get(status, "")
                    st.subheader(f"{icon} {status} ({len(group)})")
                    for _, row in group.iterrows():
                        render_job_card(row, event_table, namespace)
                    st.markdown("")

    # ── Tab 2: Trends ─────────────────────────────────────────────────────────
    with tab_trends:
        st.subheader(f"📈 Job Status Trend — {selected_runtime}")
        st.caption("Hourly job count by status over the last 24 hours")
        render_trend_chart(event_table, namespace)

    # ── Tab 3: All Runtimes Overview ──────────────────────────────────────────
    with tab_overview:
        st.subheader("🌐 All Runtimes Overview")
        st.caption("Job counts across all configured runtimes (last 24 hours)")
        render_multi_runtime_overview()


main()
