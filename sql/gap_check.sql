-- Gap check: hours missing from silver_psi, from the first reading up to the
-- latest hour that should exist (current SGT hour, allowing 20 min for publishing).
-- Expect zero rows. Any row is an hour to backfill:
--   databricks bundle run bronze_ingest_hourly -t dev --notebook-params backfill_date=YYYY-MM-DD
WITH bounds AS (
  SELECT
    min(reading_ts) AS lo,
    to_utc_timestamp(
      date_trunc('HOUR', from_utc_timestamp(current_timestamp() - INTERVAL 20 MINUTES, 'Asia/Singapore')),
      'Asia/Singapore'
    ) AS hi
  FROM workspace.hazer.silver_psi
),
expected AS (
  SELECT explode(sequence(lo, hi, INTERVAL 1 HOUR)) AS reading_ts FROM bounds
),
present AS (
  SELECT reading_ts, count(DISTINCT region) AS regions
  FROM workspace.hazer.silver_psi
  GROUP BY reading_ts
)
SELECT
  e.reading_ts,
  from_utc_timestamp(e.reading_ts, 'Asia/Singapore') AS reading_ts_sgt,
  coalesce(p.regions, 0) AS regions_present
FROM expected e
LEFT JOIN present p USING (reading_ts)
WHERE coalesce(p.regions, 0) < 5
ORDER BY e.reading_ts;