# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold transform
# MAGIC Health bands, then hourly and daily aggregates built from silver.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "hazer")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

SILVER_PSI = f"{catalog}.{schema}.silver_psi"
SILVER_PM25 = f"{catalog}.{schema}.silver_pm25"
BANDS = f"{catalog}.{schema}.ref_health_bands"

# COMMAND ----------

# Lower bound inclusive, upper bound exclusive (NULL = no upper limit).
# Exclusive upper bounds mean a decimal like 55.5 still lands in a band.
# Bands verified against NEA (nea.gov.sg air pollution FAQ for 24hr PSI,
# haze.gov.sg 1hr PM2.5 readings page) in Oct 2026.
spark.sql(f"""
CREATE OR REPLACE TABLE {BANDS} AS
SELECT * FROM VALUES
  ('psi',      1, 'Good',           0.0,   51.0),
  ('psi',      2, 'Moderate',      51.0,  101.0),
  ('psi',      3, 'Unhealthy',    101.0,  201.0),
  ('psi',      4, 'Very unhealthy', 201.0, 301.0),
  ('psi',      5, 'Hazardous',     301.0,   CAST(NULL AS DOUBLE)),
  ('pm25_1hr', 1, 'Normal',          0.0,  56.0),
  ('pm25_1hr', 2, 'Elevated',       56.0, 151.0),
  ('pm25_1hr', 3, 'High',          151.0, 251.0),
  ('pm25_1hr', 4, 'Very high',     251.0,   CAST(NULL AS DOUBLE))
AS t(metric, band_order, band_name, lower_bound, upper_bound)
""")

display(spark.table(BANDS).orderBy("metric", "band_order"))

# COMMAND ----------

# Sanity check: label the latest PSI reading for each region
display(spark.sql(f"""
SELECT s.reading_ts, s.region, s.psi_24hr, b.band_name
FROM {SILVER_PSI} s
LEFT JOIN {BANDS} b
  ON b.metric = 'psi'
 AND s.psi_24hr >= b.lower_bound
 AND (b.upper_bound IS NULL OR s.psi_24hr < b.upper_bound)
WHERE s.reading_ts = (SELECT max(reading_ts) FROM {SILVER_PSI})
ORDER BY s.region
"""))

# COMMAND ----------

GOLD_REGION = f"{catalog}.{schema}.gold_hourly_region"
GOLD_ISLAND = f"{catalog}.{schema}.gold_hourly_island"

# FULL OUTER JOIN so an hour that exists in only one endpoint is kept, not dropped.
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_REGION} AS
SELECT
  coalesce(p.reading_ts, m.reading_ts)               AS reading_ts,
  coalesce(p.reading_date_sgt, m.reading_date_sgt)   AS reading_date_sgt,
  coalesce(p.reading_hour_sgt, m.reading_hour_sgt)   AS reading_hour_sgt,
  coalesce(p.region, m.region)                       AS region,
  p.psi_24hr,
  pb.band_name   AS psi_band,
  pb.band_order  AS psi_band_order,
  m.pm25_1hr,
  mb.band_name   AS pm25_band,
  mb.band_order  AS pm25_band_order
FROM {SILVER_PSI} p
FULL OUTER JOIN {SILVER_PM25} m
  ON p.reading_ts = m.reading_ts AND p.region = m.region
LEFT JOIN {BANDS} pb
  ON pb.metric = 'psi'
 AND p.psi_24hr >= pb.lower_bound
 AND (pb.upper_bound IS NULL OR p.psi_24hr < pb.upper_bound)
LEFT JOIN {BANDS} mb
  ON mb.metric = 'pm25_1hr'
 AND m.pm25_1hr >= mb.lower_bound
 AND (mb.upper_bound IS NULL OR m.pm25_1hr < mb.upper_bound)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_ISLAND} AS
WITH agg AS (
  SELECT
    reading_ts, reading_date_sgt, reading_hour_sgt,
    max(psi_24hr)                   AS psi_max,
    round(avg(psi_24hr), 1)         AS psi_avg,
    max_by(region, psi_24hr)        AS worst_region,
    round(avg(pm25_1hr), 1)         AS pm25_avg,
    max(pm25_1hr)                   AS pm25_max,
    count(*)                        AS regions_reported
  FROM {GOLD_REGION}
  GROUP BY reading_ts, reading_date_sgt, reading_hour_sgt
)
SELECT a.*, b.band_name AS psi_band, b.band_order AS psi_band_order
FROM agg a
LEFT JOIN {BANDS} b
  ON b.metric = 'psi'
 AND a.psi_max >= b.lower_bound
 AND (b.upper_bound IS NULL OR a.psi_max < b.upper_bound)
""")

# COMMAND ----------

# Checks
r = spark.table(GOLD_REGION)
i = spark.table(GOLD_ISLAND)
print("region rows:", r.count(), "| island rows (hours):", i.count())
print("hours with fewer than 5 regions:", i.where("regions_reported < 5").count())
print("rows with null PSI band:", r.where("psi_24hr IS NOT NULL AND psi_band IS NULL").count())

display(i.orderBy("reading_ts", ascending=False).limit(6))

# COMMAND ----------

GOLD_DAILY_REGION = f"{catalog}.{schema}.gold_daily_region"
GOLD_DAILY_ISLAND = f"{catalog}.{schema}.gold_daily_island"

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_DAILY_REGION} AS
SELECT
  reading_date_sgt,
  region,
  count(*)                                              AS hours_reported,
  round(avg(psi_24hr), 1)                               AS psi_avg,
  max(psi_24hr)                                         AS psi_max,
  round(avg(pm25_1hr), 1)                               AS pm25_avg,
  max(pm25_1hr)                                         AS pm25_max,
  sum(CASE WHEN psi_band_order >= 3 THEN 1 ELSE 0 END)  AS hours_unhealthy_or_worse
FROM {GOLD_REGION}
GROUP BY reading_date_sgt, region
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_DAILY_ISLAND} AS
SELECT
  reading_date_sgt,
  count(*)                                              AS hours_reported,
  count(*) = 24                                         AS is_complete_day,
  max(psi_max)                                          AS psi_max,
  round(avg(psi_max), 1)                                AS psi_avg,
  max_by(reading_hour_sgt, psi_max)                     AS worst_hour_sgt,
  round(avg(pm25_avg), 1)                               AS pm25_avg,
  max(pm25_max)                                         AS pm25_max,
  sum(CASE WHEN psi_band_order >= 3 THEN 1 ELSE 0 END)  AS hours_unhealthy_or_worse
FROM {GOLD_ISLAND}
GROUP BY reading_date_sgt
""")

# COMMAND ----------

dr = spark.table(GOLD_DAILY_REGION)
di = spark.table(GOLD_DAILY_ISLAND)
print("daily region rows:", dr.count(), "| daily island rows (days):", di.count())
display(di.orderBy("reading_date_sgt", ascending=False))

# COMMAND ----------
# ref_regions: region label coordinates from the latest good PSI payload.
# Static data, but rebuilt each run. Guarded so an empty result never wipes the table.
BRONZE = f"{catalog}.{schema}.bronze_nea_air_quality"
REF_REGIONS = f"{catalog}.{schema}.ref_regions"

regions_sql = f"""
WITH latest AS (
  SELECT payload
  FROM {BRONZE}
  WHERE endpoint = 'psi' AND ok
  ORDER BY fetched_at DESC
  LIMIT 1
)
SELECT m.name AS region,
       m.labelLocation.latitude  AS latitude,
       m.labelLocation.longitude AS longitude
FROM latest
LATERAL VIEW explode(from_json(
  get_json_object(payload, '$.data.regionMetadata'),
  'array<struct<name:string,labelLocation:struct<latitude:double,longitude:double>>>'
)) t AS m
"""

n_regions = spark.sql(regions_sql).count()
if n_regions == 0:
    raise ValueError("ref_regions: no regionMetadata in latest PSI payload; existing table left unchanged")

spark.sql(f"CREATE OR REPLACE TABLE {REF_REGIONS} AS {regions_sql}")
print(f"ref_regions rebuilt with {n_regions} regions")