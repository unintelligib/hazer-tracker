# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver transform
# MAGIC Parses bronze JSON into clean, deduplicated silver tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "hazer")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
full_refresh = dbutils.widgets.get("full_refresh") == "true"

BRONZE = f"{catalog}.{schema}.bronze_nea_air_quality"
SILVER_PSI = f"{catalog}.{schema}.silver_psi"
SILVER_PM25 = f"{catalog}.{schema}.silver_pm25"

# COMMAND ----------

from pyspark.sql import functions as F


def read_new_bronze(endpoint: str, silver_table: str):
    """Bronze rows for one endpoint that silver hasn't seen yet."""
    df = spark.table(BRONZE).where((F.col("endpoint") == endpoint) & F.col("ok"))

    if not full_refresh and spark.catalog.tableExists(silver_table):
        watermark = spark.table(silver_table).agg(F.max("fetched_at")).first()[0]
        if watermark is not None:
            df = df.where(F.col("fetched_at") > F.lit(watermark))
    return df


new_psi = read_new_bronze("psi", SILVER_PSI)
new_pm25 = read_new_bronze("pm25", SILVER_PM25)
print("new psi bronze rows:", new_psi.count())
print("new pm25 bronze rows:", new_pm25.count())

# COMMAND ----------

from functools import reduce
from pyspark.sql import Window

REGIONS = ["north", "south", "west", "east", "central"]
REGION_STRUCT = "struct<" + ",".join(f"{r}:double" for r in REGIONS) + ">"
TS_FMT = "yyyy-MM-dd'T'HH:mm:ssXXX"  # e.g. 2026-10-17T23:00:00+08:00

# API field name -> silver column name
PM25_METRICS = {"pm25_one_hourly": "pm25_1hr"}
PSI_METRICS = {
    "psi_twenty_four_hourly": "psi_24hr",
    "pm25_twenty_four_hourly": "pm25_24hr",
    "pm10_twenty_four_hourly": "pm10_24hr",
    "so2_twenty_four_hourly": "so2_24hr",
    "no2_one_hour_max": "no2_1hr_max",
    "co_eight_hour_max": "co_8hr_max",
    "o3_eight_hour_max": "o3_8hr_max",
    "co_sub_index": "co_sub_index",
    "o3_sub_index": "o3_sub_index",
    "pm10_sub_index": "pm10_sub_index",
    "pm25_sub_index": "pm25_sub_index",
    "so2_sub_index": "so2_sub_index",
}


def payload_schema(metrics: dict) -> str:
    readings = ",".join(f"{raw}:{REGION_STRUCT}" for raw in metrics)
    return (
        "struct<data:struct<items:array<struct<"
        "date:string,updatedTimestamp:string,timestamp:string,"
        f"readings:struct<{readings}>>>>>"
    )


def parse_bronze(df, metrics: dict):
    """Bronze rows -> one row per (reading timestamp, region)."""
    parsed = df.select(
        "fetched_at", F.from_json("payload", payload_schema(metrics)).alias("p")
    )
    items = parsed.select("fetched_at", F.explode("p.data.items").alias("item"))

    per_region = [
        items.select(
            "fetched_at",
            F.col("item.timestamp").alias("ts_str"),
            F.col("item.updatedTimestamp").alias("upd_str"),
            F.lit(region).alias("region"),
            *[
                F.col(f"item.readings.{raw}.{region}").alias(name)
                for raw, name in metrics.items()
            ],
        )
        for region in REGIONS
    ]
    flat = reduce(lambda a, b: a.unionByName(b), per_region)

    return flat.select(
        F.to_timestamp("ts_str", TS_FMT).alias("reading_ts"),
        # Taken from the +08:00 string itself, so these are Singapore
        # date/hour regardless of the cluster's session time zone.
        F.to_date(F.substring("ts_str", 1, 10)).alias("reading_date_sgt"),
        F.substring("ts_str", 12, 2).cast("int").alias("reading_hour_sgt"),
        "region",
        *metrics.values(),
        F.to_timestamp("upd_str", TS_FMT).alias("updated_ts"),
        "fetched_at",
    )


def dedupe_batch(df):
    """Keep one row per (reading_ts, region): latest updated_ts, then latest fetched_at."""
    w = Window.partitionBy("reading_ts", "region").orderBy(
        F.col("updated_ts").desc_nulls_last(), F.col("fetched_at").desc()
    )
    return df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")


flat_pm25 = parse_bronze(new_pm25, PM25_METRICS)
flat_psi = parse_bronze(new_psi, PSI_METRICS)
clean_pm25 = dedupe_batch(flat_pm25)
clean_psi = dedupe_batch(flat_psi)

print("pm25 rows before/after dedupe:", flat_pm25.count(), clean_pm25.count())
print("psi rows before/after dedupe:", flat_psi.count(), clean_psi.count())

display(clean_psi.orderBy("reading_ts", "region").limit(10))

# COMMAND ----------

def upsert(df, table: str, view: str):
    """Idempotent upsert keyed on (reading_ts, region)."""
    if not spark.catalog.tableExists(table):
        df.limit(0).write.format("delta").saveAsTable(table)  # create empty, same schema

    df.createOrReplaceTempView(view)
    spark.sql(f"""
        MERGE INTO {table} AS t
        USING {view} AS s
        ON t.reading_ts = s.reading_ts AND t.region = s.region
        WHEN MATCHED AND (t.updated_ts IS NULL OR s.updated_ts >= t.updated_ts)
            THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


upsert(clean_pm25, SILVER_PM25, "stg_pm25")
upsert(clean_psi, SILVER_PSI, "stg_psi")

# COMMAND ----------

# Sanity check: row count should equal distinct (reading_ts, region) pairs
for table in (SILVER_PM25, SILVER_PSI):
    df = spark.table(table)
    print(
        table,
        "| rows:", df.count(),
        "| distinct keys:", df.select("reading_ts", "region").distinct().count(),
        "| first:", df.agg(F.min("reading_ts")).first()[0],
        "| last:", df.agg(F.max("reading_ts")).first()[0],
    )