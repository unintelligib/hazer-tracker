# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Forecast
# MAGIC Forecast island-average PM2.5 at +1, +3, +6 and +24 hours from the latest hour in `gold_hourly_island`, with a likely range, and save it to `gold_forecast`.
# MAGIC
# MAGIC - Model: ridge regression on recent lags and hour of day, refit on all history every run (it takes seconds). The method and its measured accuracy are in `04_forecast_baseline.py` and the README.
# MAGIC - `gold_forecast` keeps every run's forecast, one row per (origin hour, horizon, model version), so forecasts can later be checked against what happened. Re-running for the same origin hour replaces that hour's rows instead of adding duplicates.
# MAGIC - Not an official forecast. It is least reliable during haze.

# COMMAND ----------

import os
import sys

import pandas as pd
from pyspark.sql import types as T

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "hazer")
dbutils.widgets.text("ridge_alpha", "10", "Ridge penalty")
dbutils.widgets.text("horizons", "1,3,6,24", "Forecast horizons in hours (max 24)")
dbutils.widgets.text("max_age_hours", "3", "Skip if the latest reading is older than this")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RIDGE_ALPHA = float(dbutils.widgets.get("ridge_alpha"))
HORIZONS = [int(h) for h in dbutils.widgets.get("horizons").split(",")]
MAX_AGE_HOURS = float(dbutils.widgets.get("max_age_hours"))
assert all(1 <= h <= 24 for h in HORIZONS), "horizons must be between 1 and 24"

GOLD_ISLAND = f"{CATALOG}.{SCHEMA}.gold_hourly_island"
FORECAST_TABLE = f"{CATALOG}.{SCHEMA}.gold_forecast"
MODEL = "ridge"
MODEL_VERSION = f"ridge_v1_alpha{RIDGE_ALPHA:g}"

# Make src/ importable. Works when the repo is synced to the workspace (bundle deploy or Git folder).
try:
    notebook_dir = os.path.dirname(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    )
    src_dir = os.path.abspath(f"/Workspace{notebook_dir}/../src")
except Exception:
    src_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
sys.path.append(src_dir)

from hazer import forecast as fc  # noqa: E402

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FORECAST_TABLE} (
  origin_ts         TIMESTAMP COMMENT 'Latest reading the forecast starts from',
  origin_date_sgt   DATE,
  origin_hour_sgt   INT,
  horizon_h         INT       COMMENT 'Hours ahead',
  target_ts         TIMESTAMP COMMENT 'Hour being forecast (origin_ts + horizon_h)',
  target_date_sgt   DATE,
  target_hour_sgt   INT,
  pm25_now          DOUBLE    COMMENT 'Island-average PM2.5 at origin; also the persistence forecast',
  pm25_forecast     DOUBLE    COMMENT 'Ridge forecast, ug/m3',
  pm25_lower        DOUBLE    COMMENT 'Likely range, lower end',
  pm25_upper        DOUBLE    COMMENT 'Likely range, upper end',
  range_tail        DOUBLE    COMMENT 'Tail share on each side of the range (0.10 up to +3h, 0.05 beyond)',
  model             STRING,
  model_version     STRING,
  train_rows        INT,
  written_at        TIMESTAMP
)
COMMENT 'PM2.5 forecasts from 06_forecast. Not an official forecast.'
""")

# COMMAND ----------

raw = spark.sql(f"""
    SELECT reading_ts, reading_date_sgt, reading_hour_sgt, pm25_avg
    FROM {GOLD_ISLAND}
""").toPandas()

series, sgt_hour, off_grid = fc.hourly_series(raw)
print(f"Hours on the grid: {len(series)}, with a reading: {int(series.notna().sum())}, off the hour (dropped): {off_grid}")

origin, rows, skipped = fc.forecast_latest(series, sgt_hour, HORIZONS, RIDGE_ALPHA)
for h, reason in skipped.items():
    print(f"+{h}h skipped: {reason}")
if not rows:
    dbutils.notebook.exit("No forecast written: " + "; ".join(f"+{h}h {r}" for h, r in skipped.items()))

# Timestamps from toPandas are in the Spark session time zone, so compare with Spark's clock.
now_ts = spark.sql("SELECT current_timestamp() AS now").toPandas()["now"].iloc[0]
age_hours = (now_ts - origin) / pd.Timedelta(hours=1)
print(f"Origin: {origin} (session time zone), age {age_hours:.1f} h")
if age_hours > MAX_AGE_HOURS:
    dbutils.notebook.exit(f"No forecast written: latest reading is {age_hours:.1f} h old (limit {MAX_AGE_HOURS:g} h)")

# COMMAND ----------

origin_row = raw.loc[raw["reading_ts"] == origin].iloc[0]
origin_sgt = pd.Timestamp(origin_row["reading_date_sgt"]) + pd.Timedelta(hours=int(origin_row["reading_hour_sgt"]))

records = []
for r in rows:
    target_sgt = origin_sgt + pd.Timedelta(hours=r["horizon_h"])
    records.append((
        origin.to_pydatetime(), origin_sgt.date(), origin_sgt.hour,
        r["horizon_h"],
        (origin + pd.Timedelta(hours=r["horizon_h"])).to_pydatetime(), target_sgt.date(), target_sgt.hour,
        r["pm25_now"], r["pm25_forecast"], r["pm25_lower"], r["pm25_upper"], r["range_tail"],
        MODEL, MODEL_VERSION, r["train_rows"],
    ))

schema = T.StructType([
    T.StructField("origin_ts", T.TimestampType()),
    T.StructField("origin_date_sgt", T.DateType()),
    T.StructField("origin_hour_sgt", T.IntegerType()),
    T.StructField("horizon_h", T.IntegerType()),
    T.StructField("target_ts", T.TimestampType()),
    T.StructField("target_date_sgt", T.DateType()),
    T.StructField("target_hour_sgt", T.IntegerType()),
    T.StructField("pm25_now", T.DoubleType()),
    T.StructField("pm25_forecast", T.DoubleType()),
    T.StructField("pm25_lower", T.DoubleType()),
    T.StructField("pm25_upper", T.DoubleType()),
    T.StructField("range_tail", T.DoubleType()),
    T.StructField("model", T.StringType()),
    T.StructField("model_version", T.StringType()),
    T.StructField("train_rows", T.IntegerType()),
])
spark.createDataFrame(records, schema).createOrReplaceTempView("new_forecast")

spark.sql(f"""
MERGE INTO {FORECAST_TABLE} t
USING (SELECT *, current_timestamp() AS written_at FROM new_forecast) s
  ON t.origin_ts = s.origin_ts AND t.horizon_h = s.horizon_h AND t.model_version = s.model_version
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# Check: the forecast just written
display(spark.sql(f"""
SELECT origin_date_sgt, origin_hour_sgt, horizon_h, target_date_sgt, target_hour_sgt,
       round(pm25_now, 1) AS now, round(pm25_forecast, 1) AS forecast,
       round(pm25_lower, 1) AS lower, round(pm25_upper, 1) AS upper, model_version, written_at
FROM {FORECAST_TABLE}
WHERE origin_ts = (SELECT max(origin_ts) FROM {FORECAST_TABLE})
ORDER BY horizon_h
"""))
