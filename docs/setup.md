# Setup: Databricks Free Edition

Everything below is run from the repo root on your own computer.

## 1. Install the Databricks CLI and sign in

```bash
brew tap databricks/tap
brew trust databricks/tap
brew install databricks
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile free
```

The bundle's `free` target uses the `free` profile (see `databricks.yml`).

## 2. Store Garmin credentials as Databricks secrets

The daily job's ingest notebook (`notebooks/ingest_garmin.py`) reads credentials from a secret scope named `garmin`. Type each value at
the prompt so it never lands in your shell history:

```bash
databricks secrets create-scope garmin --profile free
databricks secrets put-secret garmin email --profile free
databricks secrets put-secret garmin password --profile free
```

Inside the job they're read with `dbutils.secrets.get("garmin", "<key>")`, and
Databricks redacts them in job output.

## 3. (Recommended) Use tokens instead of a password

A password login fails if your Garmin account has MFA enabled, because nobody is there
to type the code at 6 AM. It's also safer not to keep your password in the cloud at all.
Log in once locally and store the resulting token:

```bash
pip install garminconnect==0.3.20
python scripts/create_garmin_tokens.py
databricks secrets put-secret garmin tokens --profile free \
  --string-value "$(cat ~/.garminconnect/garmin_tokens.json)"
```

When a `tokens` secret exists, the job uses it and ignores `email` and `password`.

The secret only *seeds* the login. Garmin hands out a new refresh token every time the
short-lived access token (about a day) is refreshed, and the old one soon stops
working. So after each login the job saves the rotated tokens to
`/Volumes/workspace/garmin/auth/garmin_tokens.json` and reads that file first the next
morning. The `auth` Volume is separate from `landing` so Auto Loader never picks the
tokens up as data. Without that step the secret goes stale within a few days and every
run fails at login with `Failed to retrieve social profile`.

You can then remove the password:

```bash
databricks secrets delete-secret garmin password --profile free
```

## 4. Deploy and run

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run garmin_daily
```

This creates the following in the `workspace` catalog:

| Resource | Name |
|----------|------|
| Schema | `workspace.garmin` |
| Volume (raw JSON landing) | `/Volumes/workspace/garmin/landing` |
| Lakeflow pipeline | `garmin-medallion` |
| Job | `garmin-daily`, daily at **6:00 AM America/Chicago** |

## 5. Verify the Garmin field mappings

Garmin's API is unofficial and undocumented, so the JSON paths in
`src/garmin_lakehouse/transforms.py` (`FIELDS`) are based on the `garminconnect`
library's typical responses. After the first run, check them against your real data:

```sql
SELECT endpoint, calendar_date, payload
FROM workspace.garmin.bronze_garmin_raw
WHERE payload IS NOT NULL
ORDER BY calendar_date DESC
LIMIT 20;
```

A silver column that's entirely `NULL` while the payload clearly has the value means
its path needs updating in `FIELDS`. Bronze never needs a change, because it stores
the payload as raw text.

## Troubleshooting: the daily job stopped

Check the run history in **Jobs & Pipelines → garmin-daily**. Free Edition purges
failed runs from that list after a while, so if it looks empty, query the system tables:

```sql
SELECT period_start_time, task_key, result_state, termination_code
FROM system.lakeflow.job_task_run_timeline
WHERE job_id = '<garmin-daily job id>'
ORDER BY period_start_time DESC
LIMIT 20;
```

- **`ingest` FAILED, `medallion` SKIPPED, error `Failed to retrieve social profile`:**
  every saved token has expired (for example after the job didn't run for a long time).
  Regenerate them with step 3, then run the job once. The job falls back to the fresh
  secret and rewrites the token store.
- **No runs at all, schedule still `UNPAUSED`:** the Free Edition scheduler has twice
  stopped starting runs without logging a pause or an error: after Nov 2 (when ingest
  was failing) and after Nov 15 (when every run was succeeding). Both times it was 4–5
  days after anyone last used the workspace, so it looks like Free Edition stops
  scheduling in an idle workspace. Databricks doesn't document this, so treat it as an
  observation, not a rule. Redeploying (`databricks bundle deploy`) re-saves the
  schedule; then run the job once, since the default 7-day pull backfills the gap.

## Keep the daily run going (Free Edition)

The Free Edition scheduler can stop starting the 6 AM run after a few days without
anyone using the workspace (see the troubleshooting section above).
`scripts/ensure_daily_run.py` checks later in the morning and starts the job if no run
succeeded today. It starts at most one run a day, and never retries a manual run that
already failed.

1. **Create a personal access token profile.** The normal CLI login expires after about
   a week, which is too short for an unattended check. In the workspace, open
   **Settings → Developer → Access tokens → Generate new token** (lifetime 90 days), then
   save it as its own profile, pasting the token at the prompt:

   ```bash
   databricks configure --profile free-pat --host https://<workspace>.cloud.databricks.com
   ```

2. **Try it:** `python3 scripts/ensure_daily_run.py --profile free-pat --dry-run`

3. **Schedule it with launchd (macOS)** at 9:07 every morning: fill in
   `scripts/ensure_daily_run.plist.template` (repo path, the folder of `which databricks`
   and `~/Library/Logs`), save it as
   `~/Library/LaunchAgents/com.garmin-lakehouse.ensure-daily-run.plist`, then run
   `launchctl load` on it. If the Mac is asleep at 9:07, launchd runs it at wake-up.
   Each run adds one line to `~/Library/Logs/garmin-ensure-daily-run.log`.

Renew the token before it expires (the log shows `ERROR` once it has).

## Fallback: ingest from your laptop

If serverless compute can't reach Garmin from your workspace, run ingestion locally and
upload the files. The pipeline doesn't care where they came from.

```bash
export GARMINTOKENS="$(cat ~/.garminconnect/garmin_tokens.json)"
PYTHONPATH=src python -m garmin_lakehouse.ingest --landing ./data/landing --days 30
databricks fs cp -r ./data/landing dbfs:/Volumes/workspace/garmin/landing --overwrite --profile free
```

`data/` is in `.gitignore`, so your health data never ends up in git.
