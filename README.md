# Hazer Tracker

Near-real-time Singapore haze dashboard on Databricks: PSI and PM2.5 by region from NEA (via data.gov.sg), modelled with a bronze / silver / gold Delta pipeline.

> Status: Week 1 (ingest). Architecture diagram, screenshots and demo come in Week 4.

## Architecture

```
data.gov.sg (NEA PSI + PM2.5)
        │  hourly scheduled job (retries, failure rows kept)
        ▼
bronze_nea_air_quality   raw JSON, append-only, one row per request
        │  flatten by region, parse to SGT, dedupe        (Week 2)
        ▼
silver_*                 one row per (reading time, region, metric)
        │  hourly / daily averages, health bands, flags   (Week 2)
        ▼
gold_*                   dashboard-ready tables
        ▼
Databricks SQL dashboard                                  (Week 3)
```

## Data source

| Endpoint | Content |
|---|---|
| `https://api-open.data.gov.sg/v2/real-time/api/psi` | 24h PSI, sub-indices and pollutant concentrations by region |
| `https://api-open.data.gov.sg/v2/real-time/api/pm25` | 1-hour PM2.5 (µg/m³) by region |

* Regions: north, south, east, west, central (with label coordinates in `regionMetadata`, handy for the map).
* No API key needed; an optional `x-api-key` header gives higher rate limits. Without a key, 429s are possible, so the client backs off and retries.
* `?date=YYYY-MM-DD` returns a whole SGT day (used for backfill); omitting it returns the latest reading.
* Timestamps are ISO 8601 with a `+08:00` offset. PSI and PM2.5 can carry different latest timestamps at the same moment, so never join them on fetch time. Join on reading timestamp.

## Bronze table

`<catalog>.<schema>.bronze_nea_air_quality`

| Column | Notes |
|---|---|
| `endpoint` | `psi` or `pm25` |
| `request_date` | `YYYY-MM-DD` when backfilling, else null |
| `page` | page number within one fetch run |
| `fetched_at` | UTC instant of the request |
| `http_status`, `ok`, `error` | failed fetches are stored too, so gaps are visible |
| `payload` | raw JSON body, untouched |

## Setup

1. Install the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) and run `databricks auth login --host <workspace-url>`.
2. `databricks bundle deploy -t dev` deploys the notebook and the hourly job.
3. Run the job once from the Jobs UI and check `SELECT * FROM workspace.hazer.bronze_nea_air_quality`.
4. Optional backfill: run the notebook with `backfill_date` set to a past date.

Local tests (no Databricks needed):

```bash
pip install -r requirements.txt
pytest
```

## Open questions

* Pagination: the docs describe a pagination token for date-filtered requests, but its exact response field name has not been confirmed against a live multi-page response. The client accepts `paginationToken` or `pagination_token`. Confirm on the first real backfill.
* Free Edition limits on scheduled jobs and public dashboard sharing: confirm in the workspace.
