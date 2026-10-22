# Design choices

Why the project is built the way it is, including a few things that did not go as expected.

## Pipeline

**Bronze keeps everything, including failures.** Every API request becomes one row in `bronze_nea_air_quality`, with the raw JSON untouched and the HTTP status, an `ok` flag and any error message. The client never raises on a failed fetch. A failed run is therefore visible as data, so gaps can be found with a query and not only noticed as a red job.

**The live endpoint only returns the latest reading.** A missed hourly run leaves a hole until that date is backfilled with the `backfill_date` parameter (`?date=YYYY-MM-DD` returns a whole SGT day). `sql/gap_check.sql` looks for missing hours in `silver_psi` and should return no rows. It does not cover `silver_pm25` yet.

**Silver is incremental, gold is rebuilt.** Silver merges on (`reading_ts`, `region`), so re-running or backfilling never duplicates rows. Gold is rebuilt with `CREATE OR REPLACE` on every run, which means a backfilled gap flows through to the dashboard on the next run with no special handling. At this data size a full rebuild is cheap and removes a whole class of "is gold up to date?" bugs.

**Task dependencies.** The job runs `ingest` -> `silver` -> `gold`. Silver uses `run_if: ALL_DONE`, so it still runs if ingest fails and picks up whatever bronze already holds. Gold runs only if silver succeeds. Only one run can be active at a time (`max_concurrent_runs: 1`) and a run is cut off after 600 seconds. Normal runs take about a minute or two.

**Time zones.** Source timestamps carry a `+08:00` offset. Silver stores the reading as a UTC instant plus separate SGT date and hour columns, and PSI and PM2.5 are joined on reading timestamp, never on fetch time, because the two endpoints can have different latest timestamps at the same moment. The dashboard datasets use the SGT columns, so charts do not shift with the viewer's or the warehouse's time zone.

**Health bands are data, not code.** Band names and bounds live in `ref_health_bands` and are rebuilt each gold run. PSI bands were checked against NEA's published scale.

## The high-haze flag is PSI-based

`is_high_haze` is true when the island PSI is in the "Very unhealthy" band or worse. The band is looked up from `ref_health_bands`, not hardcoded as 201. The flag uses PSI because NEA's health advisories apply to the 24-hour PSI. The PM2.5 1-hour bands are not tied to those advisories, so flagging on PM2.5 would imply an official alert that does not exist.

On the dashboard, the alert counter shows "VERY UNHEALTHY OR WORSE" when the flag is on. The column and flag keep the technical name `is_high_haze`.

## Dashboard on Free Edition

**Built in the UI, then exported.** The AI/BI dashboard was built by hand and published, then exported into the bundle with `databricks bundle generate dashboard`. Free Edition has no way to create extra SQL warehouses, so it uses the serverless starter warehouse. `bundle deploy` does create a dashboard on Free Edition, which was an open question until it was tried.

**Things that surprised me:**

* **Two dashboards, one name.** `bundle deploy` created a second dashboard instead of attaching to the hand-built one (I did not use `--bind`). The deployed copy now has the display name `[${bundle.target}] Singapore Air Quality` so the two are easy to tell apart. Re-exporting overwrites that line in the YAML, so after every export I have to restore the prefix.
* **The refresh schedule lives only on the hand-published copy.** The hourly refresh (at 30 minutes past) was set from the published view. It is not part of the bundle, so a freshly deployed copy has no schedule of its own.
* **The warehouse ID is hardcoded** in `resources/singapore_air_quality.dashboard.yml`. Anyone cloning the repo has to replace it with their own. Turning it into a bundle variable is a possible cleanup.
* **Edits are not live until published.** Changes to the draft only go live after clicking Publish again, and the repo copy only changes after a re-export.
* **Counters can mislead.** A numeric counter defaults to SUM, which once inflated PSI to 1079 because a dataset still held an old test query. I now check each dataset's SQL and row count in the Data tab.

**Map colours follow NEA's scale.** NEA's PSI gauge shows Good and Moderate as one green band, then yellow for Unhealthy, orange for Very Unhealthy and red for Hazardous. The map was recoloured to match. The panel only lists bands present in the data, so Good, Very unhealthy and Hazardous get a colour only once they appear.

## Secrets and public-repo hygiene

The alert address for failure emails is a bundle variable (`alert_email`) supplied through the `BUNDLE_VAR_alert_email` environment variable, so it is never in a file in the repo. Before making the repo public I checked tracked files for email addresses and junk files, and rewrote the commit history so that commits carry GitHub's no-reply address.

## Known limitations and open questions


* Pagination follows the data.gov.sg OpenAPI spec: the token is read from `data.paginationToken` and sent back as the `paginationToken` query parameter. No whole-day request has needed a second page so far. The client also accepts `pagination_token` as a fallback, but only the documented spelling is tested.
* Public dashboard sharing on Free Edition has not been checked.
* The ingest client opens a new HTTP session for every request. Sharing one session would be a small improvement.