# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Bronze backfill over a date range
# MAGIC
# MAGIC One-off helper, **not part of the hourly job**. Pulls whole Singapore days from the PSI and PM2.5 endpoints for `start_date` to `end_date` and appends them to bronze, a few days at a time, so a failure or timeout loses at most one chunk.
# MAGIC
# MAGIC - **Safe to re-run.** With `skip_existing = true` it skips every (endpoint, date) that already has a good bronze row, so it resumes where it stopped.
# MAGIC - **No silent empty days.** A response with no readings dated the requested day is stored as a failed fetch (`ok = false`), not a good one. The API can answer with the latest reading when a date filter is not honoured, and that must not count as a backfilled day.
# MAGIC - **It stops by itself** after `max_minutes` and prints the date to resume from.
# MAGIC - **After it finishes, rebuild silver and gold** (last cell).

# COMMAND ----------

import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "hazer", "Schema")
dbutils.widgets.text("start_date", "", "First SGT date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "Last SGT date, inclusive (YYYY-MM-DD)")
dbutils.widgets.text("chunk_days", "5", "Days fetched before each write")
dbutils.widgets.text("max_days", "60", "Safety cap on days per run")
dbutils.widgets.text("max_minutes", "20", "Stop starting new chunks after this many minutes")
dbutils.widgets.text("pause_seconds", "0.3", "Pause between requests")
dbutils.widgets.dropdown("skip_existing", "true", ["true", "false"], "Skip dates already in bronze")
dbutils.widgets.text("api_key_secret", "", "Secret as scope/key (optional)")


def parse_date(name):
    raw = dbutils.widgets.get(name).strip()
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise ValueError(f"Set the {name} widget to a date like 2025-10-20 (got {raw!r})")


CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_nea_air_quality"
START = parse_date("start_date")
END = parse_date("end_date")
CHUNK_DAYS = max(1, int(dbutils.widgets.get("chunk_days")))
MAX_DAYS = int(dbutils.widgets.get("max_days"))
MAX_MINUTES = float(dbutils.widgets.get("max_minutes"))
PAUSE_SECONDS = float(dbutils.widgets.get("pause_seconds"))
SKIP_EXISTING = dbutils.widgets.get("skip_existing") == "true"

TODAY_SGT = (datetime.now(timezone.utc) + timedelta(hours=8)).date()
if START > END:
    raise ValueError(f"start_date {START} is after end_date {END}")
if END >= TODAY_SGT:
    raise ValueError(
        f"end_date must be before today in Singapore ({TODAY_SGT}). Today's readings come from the hourly job."
    )
N_DAYS = (END - START).days + 1
if N_DAYS > MAX_DAYS:
    raise ValueError(
        f"{N_DAYS} days requested, above the max_days cap of {MAX_DAYS}. "
        "Raise max_days on purpose, or split the range."
    )
print(f"Backfilling {START} to {END} ({N_DAYS} days), {CHUNK_DAYS} days per write, stop after {MAX_MINUTES} min")

# Make src/ importable. Works when the repo is synced to the workspace (bundle deploy or Git folder).
try:
    notebook_dir = os.path.dirname(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    )
    src_dir = os.path.abspath(f"/Workspace{notebook_dir}/../src")
except Exception:
    src_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
sys.path.append(src_dir)

from hazer import nea_client  # noqa: E402

api_key = None
secret_ref = dbutils.widgets.get("api_key_secret").strip()
if secret_ref:
    scope, key = secret_ref.split("/", 1)
    api_key = dbutils.secrets.get(scope, key)

# COMMAND ----------

from pyspark.sql import types as T

ENDPOINTS = list(nea_client.ENDPOINTS)

bronze_schema = T.StructType(
    [
        T.StructField("endpoint", T.StringType(), False),
        T.StructField("request_date", T.StringType(), True),
        T.StructField("page", T.IntegerType(), False),
        T.StructField("fetched_at", T.TimestampType(), False),  # UTC instant
        T.StructField("http_status", T.IntegerType(), True),
        T.StructField("ok", T.BooleanType(), False),
        T.StructField("payload", T.StringType(), True),  # raw JSON, untouched
        T.StructField("error", T.StringType(), True),
    ]
)


def on_day(res, day):
    """Reading timestamps in a response that are dated `day` (the +08:00 text itself, so Singapore time)."""
    stamps = nea_client.reading_timestamps(res.payload or "")
    return [s for s in stamps if s[:10] == day]


def check_result(res, day):
    """Return (row, ok). A 'good' response with no readings dated `day` is stored as a failed fetch."""
    row = res.as_row()
    if not res.ok or on_day(res, day):
        return row, res.ok, res.error
    n_items = len(nea_client.reading_timestamps(res.payload or ""))
    msg = f"no readings dated {day} in response ({n_items} item(s) returned)"
    names = [f.name for f in bronze_schema.fields]
    if isinstance(row, dict):
        row = {**row, "ok": False, "error": msg}
    else:
        row = list(row)
        row[names.index("ok")] = False
        row[names.index("error")] = msg
        row = tuple(row)
    return row, False, msg


def existing_pairs():
    """(endpoint, request_date) pairs that already have a good bronze row in the range."""
    if not spark.catalog.tableExists(BRONZE_TABLE):
        return set()
    rows = spark.sql(f"""
        SELECT DISTINCT endpoint, request_date FROM {BRONZE_TABLE}
        WHERE ok AND request_date BETWEEN '{START}' AND '{END}'
    """).collect()
    return {(r["endpoint"], r["request_date"]) for r in rows}


def write_bronze(batch):
    df = spark.createDataFrame(batch, schema=bronze_schema)
    df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    return len(batch)

# COMMAND ----------

days = [START + timedelta(days=i) for i in range(N_DAYS)]
done = existing_pairs() if SKIP_EXISTING else set()
print(f"{len(done)} (endpoint, date) pair(s) already in bronze for this range" if SKIP_EXISTING else "skip_existing is off")
deadline = time.monotonic() + MAX_MINUTES * 60

readings = {}   # (endpoint, date) -> readings dated that day, across pages
problems = []   # one line per failed fetch
rows_written = 0
stopped_at = None

for i in range(0, len(days), CHUNK_DAYS):
    chunk = days[i : i + CHUNK_DAYS]
    if time.monotonic() > deadline:
        stopped_at = chunk[0]
        break
    batch = []
    for d in chunk:
        ds = d.isoformat()
        for endpoint in ENDPOINTS:
            if (endpoint, ds) in done:
                continue
            for res in nea_client.fetch_all_pages(endpoint, date=ds, api_key=api_key):
                row, ok, err = check_result(res, ds)
                batch.append(row)
                if ok:
                    readings[(endpoint, ds)] = readings.get((endpoint, ds), 0) + len(on_day(res, ds))
                else:
                    problems.append(f"{endpoint} {ds} page {res.page}: {err}")
            time.sleep(PAUSE_SECONDS)
    if batch:
        rows_written += write_bronze(batch)
    print(f"{chunk[0]} to {chunk[-1]}: wrote {len(batch)} row(s)")

# COMMAND ----------

import pandas as pd

print(f"Rows written: {rows_written}   Failed fetches: {len(problems)}")
if stopped_at:
    print(f"Stopped at the time limit. Set start_date to {stopped_at} and run again; finished dates are skipped.")
for line in problems:
    print("  FAILED", line)

if readings:
    table = (
        pd.DataFrame([{"date": d, "endpoint": e, "readings": n} for (e, d), n in readings.items()])
        .pivot(index="date", columns="endpoint", values="readings")
        .sort_index()
    )
    display(table)
    partial = table[(table < 24).any(axis=1)]
    if len(partial):
        print(f"{len(partial)} day(s) with fewer than 24 readings in at least one endpoint (the API may simply lack those hours):")
        print(partial.to_string())
else:
    print("No new readings were fetched in this run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now rebuild silver and gold
# MAGIC
# MAGIC Silver only reads bronze rows fetched after the newest row it has already processed. The next hourly run normally picks up everything written here, but rows written late can carry an earlier fetch time than a row an hourly run processed in the meantime, and silver would then skip them. A full refresh re-reads all of bronze. It is safe because silver merges on (`reading_ts`, `region`).
# MAGIC
# MAGIC On the Mac, when no hourly run is due in the next few minutes (they start at :20):
# MAGIC
# MAGIC ```
# MAGIC databricks bundle run bronze_ingest_hourly -t dev --notebook-params full_refresh=true
# MAGIC ```
# MAGIC
# MAGIC That runs ingest (one normal latest-reading fetch), then silver with `full_refresh=true`, then gold. Check the result with the day-count query on `silver_pm25`.