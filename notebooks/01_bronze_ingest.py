# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze ingest: NEA PSI and PM2.5
# MAGIC
# MAGIC Calls the data.gov.sg real-time PSI and PM2.5 endpoints and **appends the raw
# MAGIC response** to a Delta table. One row per request (and per page when backfilling),
# MAGIC including failed requests, so silver can see gaps.
# MAGIC
# MAGIC * Scheduled hourly by the job in `databricks.yml`.
# MAGIC * Re-running is safe: bronze is append-only and silver deduplicates on
# MAGIC   `(endpoint, reading timestamp)`.
# MAGIC * **Backfill:** set the `backfill_date` widget (YYYY-MM-DD) to pull every reading
# MAGIC   for that SGT day instead of just the latest one.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "hazer", "Schema")
dbutils.widgets.text("backfill_date", "", "Backfill date (YYYY-MM-DD, blank = latest)")
dbutils.widgets.text("api_key_secret", "", "Secret as scope/key (optional)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BACKFILL_DATE = dbutils.widgets.get("backfill_date").strip() or None
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_nea_air_quality"

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

# COMMAND ----------

# Optional API key for higher rate limits. Never hardcode it: store it in a secret scope.
api_key = None
secret_ref = dbutils.widgets.get("api_key_secret").strip()
if secret_ref:
    scope, key = secret_ref.split("/", 1)
    api_key = dbutils.secrets.get(scope, key)

# COMMAND ----------

results = []
for endpoint in nea_client.ENDPOINTS:
    results.extend(nea_client.fetch_all_pages(endpoint, date=BACKFILL_DATE, api_key=api_key))

for r in results:
    stamps = nea_client.reading_timestamps(r.payload or "")
    print(f"{r.endpoint:5} page={r.page} ok={r.ok} status={r.http_status} readings={stamps} error={r.error}")

# COMMAND ----------

from pyspark.sql import types as T

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

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
df = spark.createDataFrame([r.as_row() for r in results], schema=bronze_schema)
df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
print(f"Appended {df.count()} row(s) to {BRONZE_TABLE}")

# COMMAND ----------

# Write first, fail after: failed fetches are already recorded above, and raising here
# makes the job run show as failed so notifications fire.
failed = [r for r in results if not r.ok]
if failed:
    summary = "; ".join(f"{r.endpoint} p{r.page}: {r.error}" for r in failed)
    raise RuntimeError(f"{len(failed)} fetch(es) failed after retries: {summary}")
