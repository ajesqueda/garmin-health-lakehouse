# garmin-health-lakehouse

A bronze → silver → gold lakehouse on **Databricks** for my own Garmin Connect health
data (sleep, HRV, resting heart rate, stress, body battery, training readiness and
workouts). It's built as a **Lakeflow declarative pipeline**, deployed with a
**Databricks Asset Bundle**, and runs every morning at 6 AM.

The gold table is designed as the feature set for the next phase: a model that scores
daily recovery and sends a text with a suggested workout.

## Problem

My watch collects a lot of health data, but it's locked in an app as separate screens
(sleep, HRV, stress, workouts). To answer "how recovered am I today compared with my
normal?" those signals have to be joined day by day and compared against my own
recent baseline. That's a data engineering problem before it's an ML problem: the
data needs reliable daily ingestion, deduplication, typing and history.

## Approach

```
Garmin Connect ──(garminconnect, 6 AM job)──▶ UC Volume: landing/<endpoint>/<date>.json
                                                 │  Auto Loader (incremental: new files only)
                                                 ▼
 BRONZE  bronze_garmin_raw          STREAMING TABLE, append-only, Change Data Feed on;
                                    one row per file, payload kept as raw JSON text
                                                 │  stream → parse (temporary views)
                                                 ▼  Auto CDC (create_auto_cdc_flow)
 SILVER  silver_daily_summary  silver_sleep  silver_hrv  silver_training_readiness
                                    STREAMING TABLES, SCD TYPE 2 keyed on calendar_date,
                                    ordered by fetched_at; a new version only when a
                                    value changes. silver_activities: SCD type 1.
                                                 │  current rows (__END_AT IS NULL)
                                                 ▼
 GOLD    gold_daily_recovery        MATERIALIZED VIEW, one row per day: today's signals
                                    vs my previous 7-day baseline + acute:chronic load
```

Design decisions:

- **Bronze never parses the payload.** Garmin's API is unofficial and changes without
  notice. Storing raw JSON text means a renamed field can't break ingestion. Only
  silver's field map (`FIELDS` in `transforms.py`) needs updating.
- **One file per endpoint per day, re-fetched for recent days.** Garmin keeps updating
  "today" and "last night" as the watch syncs. The last 2 days are re-pulled each run,
  and bronze keeps every version. Days with no data get a `null` marker file so they
  aren't requested forever.
- **Incremental, not full scans.** Garmin's API has no change feed, so the source can't
  provide CDC. The pipeline builds its own instead:
  - Auto Loader only reads landing files it hasn't seen before.
  - Bronze is an append-only streaming table with Change Data Feed enabled.
  - Silver uses **Auto CDC with SCD type 2**: every re-fetch is upserted by key and
    ordered by `fetched_at`.
  - `track_history_except_column_list=["fetched_at"]` makes an identical re-fetch a
    no-op, so a new version (with `__START_AT`/`__END_AT`) only opens when Garmin
    actually revises a value, for example last night's sleep after a late sync.
  - Gold reads only the current rows.
- **Parsing is stream-safe.** `parse_*` functions use no windows or aggregations. A
  local test runs each one on a real Spark stream, and another test proves the old
  `row_number()` dedup would be rejected on a stream.
- **Baselines exclude the current day.** "HRV 20% below baseline" compares last night
  with the 7 nights before it, not with a window that includes itself.
- **Logic is separate from orchestration.** `pipelines/garmin_pipeline.py` (a
  Databricks notebook in source format, so it opens as a notebook in the UI) only
  declares tables, flows and expectations. All transformations are plain PySpark functions that are
  unit-tested locally on synthetic data.
- **No credentials or health data in git.** Garmin credentials live in a Databricks
  secret scope, real data lives only in Databricks, and tests use synthetic payloads.

### Gold columns (`gold_daily_recovery`)

| Group | Columns |
|-------|---------|
| Sleep | `sleep_score`, `sleep_hours`, `deep_sleep_pct`, `rem_sleep_pct`, `avg_sleep_stress`, `sleep_score_avg_7d` |
| HRV | `hrv_last_night`, `hrv_weekly_avg`, `hrv_status`, `hrv_baseline_7d`, `hrv_vs_baseline_pct` |
| Heart rate | `resting_hr`, `resting_hr_baseline_7d`, `resting_hr_delta` |
| Daily load | `avg_stress`, `total_steps`, `body_battery_high/low/charged`, `readiness_score`, `readiness_level` |
| Training | `training_load`, `activity_minutes`, `activity_count`, `acute_load_7d`, `chronic_load_weekly_28d`, `acute_chronic_ratio` |
| Trust | `baseline_days`: how many of the previous 7 days back the baselines |

## Results

First production run on Databricks Free Edition (serverless), 2026-10-27:

| Layer | Rows | Notes |
|-------|------|-------|
| Landing (Volume) | 60 files | 5 endpoints × 7 days, plus re-fetched versions of the 2 most recent days |
| `bronze_garmin_raw` | 60 | every file, payload kept as raw JSON |
| Silver | 7–11 per table | 11 activities; 7–8 days for sleep, HRV, readiness and daily summary |
| `gold_daily_recovery` | 8 days | baselines fill in as history builds (`baseline_days` 0 → 7) |

**Field coverage:** every mapped silver column was populated on 100% of days that had
data. The JSON paths in `FIELDS` held up against real Garmin payloads.

**End-to-end runtime:** about 3 minutes for ingest plus the pipeline refresh, on serverless
compute.

Problems the first runs exposed, each fixed:

1. **`garminconnect` needs Python 3.12.** Serverless environment 2 runs 3.11, so the
   job now pins environment version 4.
2. **MFA blocks password logins.** An unattended 6 AM job can't type a code, so the job
   uses saved tokens from a one-time interactive login (`scripts/create_garmin_tokens.py`).
3. **Serverless clocks run on UTC.** At 7 PM in Kansas the job thought it was already
   tomorrow. "Today" is now computed in `America/Chicago`.
4. **`raise SystemExit(0)` fails a Databricks task.** Databricks treats any
   `SystemExit` as a failure, so the entry point only raises on a real error.

## How to run

**Locally (tests only, no Garmin account needed):**

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/ruff check . && .venv/bin/pytest
```

The tests start a local Spark session, so they need Java 17 or newer.

**On Databricks Free Edition:** see [docs/setup.md](docs/setup.md). In short:

```bash
databricks auth login --host https://<workspace>.cloud.databricks.com --profile free
databricks secrets create-scope garmin --profile free
databricks secrets put-secret garmin email --profile free
databricks secrets put-secret garmin password --profile free   # or a `tokens` secret
databricks bundle deploy
databricks bundle run garmin_daily
```

## Layout

```
databricks.yml                 bundle: schema, volume, pipeline, 6 AM job
pipelines/garmin_pipeline.py   notebook: Lakeflow tables, Auto CDC flows, expectations
notebooks/ingest_garmin.py     notebook: job task 1; reads secrets, pulls Garmin data
src/garmin_lakehouse/
  ingest.py                    Garmin -> landing JSON (idempotent, rate-limit aware)
  transforms.py                bronze parse, stream-safe silver parse, gold features
scripts/
  create_garmin_tokens.py      one-time local login that saves reusable tokens
tests/                         pytest + local Spark on synthetic data
```

## Roadmap

Tracked in [GitHub Issues](https://github.com/ajesqueda/garmin-health-lakehouse/issues):
verify field mappings on real data, backfill history, build a recovery score model on
an open-source model, and send a daily text with the score and a suggested workout.

## Status

- ✅ Phase 1, medallion pipeline: deployed and running daily at 6:00 AM Central
- ⏳ Phase 2, recovery score model ([#5](https://github.com/ajesqueda/garmin-health-lakehouse/issues/5), [#6](https://github.com/ajesqueda/garmin-health-lakehouse/issues/6))
- ⏳ Phase 3, daily text with suggested workout ([#7](https://github.com/ajesqueda/garmin-health-lakehouse/issues/7), [#8](https://github.com/ajesqueda/garmin-health-lakehouse/issues/8))
