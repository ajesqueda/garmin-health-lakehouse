"""Bronze/silver/gold on synthetic payloads shaped like Garmin Connect responses.

No real health data is used anywhere in this repo.
"""

import json
from datetime import date, timedelta

import pytest
from conftest import envelope

from garmin_lakehouse.transforms import (
    FIELDS,
    gold_daily_recovery,
    silver_activities,
    silver_daily_summary,
    silver_hrv,
    silver_sleep,
    silver_training_readiness,
)

D0 = date(2026, 10, 1)


def day(n: int) -> str:
    return (D0 + timedelta(days=n)).isoformat()


def summary(rhr=50, steps=8000, stress=30):
    return {
        "totalSteps": steps, "restingHeartRate": rhr, "averageStressLevel": stress,
        "bodyBatteryHighestValue": 90, "bodyBatteryLowestValue": 20,
        "bodyBatteryChargedValue": 60, "unexpectedNewField": {"ignored": True},
    }


def sleep(total=28800, deep=5760, rem=7200, score=80):
    return {"dailySleepDTO": {
        "sleepTimeSeconds": total, "deepSleepSeconds": deep, "remSleepSeconds": rem,
        "lightSleepSeconds": total - deep - rem, "sleepScores": {"overall": {"value": score}},
    }}


def hrv(last_night=50, status="BALANCED"):
    return {"hrvSummary": {"lastNightAvg": last_night, "weeklyAvg": 52, "status": status,
                           "baseline": {"balancedLow": 45, "balancedUpper": 60}}}


def activity(activity_id, start, load=100.0, minutes=60):
    return {"activityId": activity_id, "activityName": "Run", "startTimeLocal": start,
            "activityType": {"typeKey": "running"}, "duration": minutes * 60.0,
            "activityTrainingLoad": load}


# --- bronze ---------------------------------------------------------------


def test_bronze_keeps_payload_as_raw_json_text(make_bronze):
    row = make_bronze([envelope("sleep", day(0), sleep())]).collect()[0]
    assert row.endpoint == "sleep"
    assert row.calendar_date == D0
    assert json.loads(row.payload)["dailySleepDTO"]["sleepTimeSeconds"] == 28800
    assert row.source_file == "/landing/file_0.json"
    assert row.ingested_at is not None


def test_bronze_null_payload_marker_becomes_sql_null(make_bronze):
    row = make_bronze([envelope("hrv", day(0), None)]).collect()[0]
    assert row.payload is None


# --- silver ---------------------------------------------------------------


def test_daily_summary_types_and_ignores_unknown_fields(make_bronze):
    df = silver_daily_summary(make_bronze([envelope("daily_summary", day(0), summary(rhr=47))]))
    row = df.collect()[0]
    assert (row.resting_hr, row.total_steps, row.body_battery_high) == (47, 8000, 90)
    assert row.max_stress is None  # field absent from the payload -> null, not an error
    assert set(FIELDS["daily_summary"]) <= set(df.columns)


def test_latest_fetch_wins_per_day(make_bronze):
    bronze = make_bronze([
        envelope("daily_summary", day(0), summary(steps=3000), "2026-10-01T09:00:00+00:00"),
        envelope("daily_summary", day(0), summary(steps=12000), "2026-10-01T22:00:00+00:00"),
    ])
    rows = silver_daily_summary(bronze).collect()
    assert [r.total_steps for r in rows] == [12000]


def test_sleep_derives_hours_and_stage_percentages(make_bronze):
    row = silver_sleep(make_bronze([envelope("sleep", day(0), sleep())])).collect()[0]
    assert row.sleep_hours == 8.0
    assert row.deep_sleep_pct == 20.0
    assert row.rem_sleep_pct == 25.0
    assert row.sleep_score == 80


def test_silver_skips_no_data_markers(make_bronze):
    bronze = make_bronze([envelope("hrv", day(0), None), envelope("hrv", day(1), hrv(55))])
    assert [r.hrv_last_night for r in silver_hrv(bronze).collect()] == [55]


def test_training_readiness_keeps_latest_score_of_the_day(make_bronze):
    payload = [
        {"score": 40, "level": "LOW", "timestamp": "2026-10-01T06:00:00"},
        {"score": 65, "level": "MODERATE", "timestamp": "2026-10-01T14:00:00"},
    ]
    rows = silver_training_readiness(
        make_bronze([envelope("training_readiness", day(0), payload)])
    ).collect()
    assert [(r.readiness_score, r.readiness_level) for r in rows] == [(65, "MODERATE")]


def test_activities_explode_and_refetch_does_not_duplicate(make_bronze):
    two = [activity(1, f"{day(0)} 06:30:00"), activity(2, f"{day(0)} 18:00:00", load=40)]
    bronze = make_bronze([
        envelope("activities", day(0), two, "2026-10-01T20:00:00+00:00"),
        envelope("activities", day(0), two, "2026-10-02T06:00:00+00:00"),  # refetch
    ])
    rows = sorted(silver_activities(bronze).collect(), key=lambda r: r.activity_id)
    assert [r.activity_id for r in rows] == [1, 2]
    assert rows[0].activity_type == "running"
    assert rows[0].activity_date == D0
    assert rows[1].training_load == 40.0


# --- gold -----------------------------------------------------------------


@pytest.fixture
def gold(make_bronze):
    """28 days of history. HRV and resting HR are flat until a bad last day."""
    files = []
    for n in range(28):
        last = n == 27
        files += [
            envelope("daily_summary", day(n), summary(rhr=56 if last else 50)),
            envelope("sleep", day(n), sleep(score=60 if last else 80)),
            envelope("hrv", day(n), hrv(40 if last else 50)),
            envelope("activities", day(n), [activity(n + 1, f"{day(n)} 07:00:00", load=100)]),
        ]
    files.append(envelope("training_readiness", day(27), [{"score": 35, "level": "LOW"}]))
    bronze = make_bronze(files)
    df = gold_daily_recovery(
        silver_daily_summary(bronze), silver_sleep(bronze), silver_hrv(bronze),
        silver_training_readiness(bronze), silver_activities(bronze),
    )
    return {r.calendar_date: r for r in df.collect()}


def test_gold_has_one_row_per_day(gold):
    assert sorted(gold) == [D0 + timedelta(days=n) for n in range(28)]


def test_gold_compares_last_night_with_previous_week(gold):
    last = gold[D0 + timedelta(days=27)]
    assert last.hrv_baseline_7d == 50.0  # the bad night itself is not in its own baseline
    assert last.hrv_vs_baseline_pct == -20.0
    assert last.resting_hr_delta == 6.0
    assert last.baseline_days == 7
    assert last.readiness_score == 35


def test_gold_baseline_needs_history(gold):
    first = gold[D0]
    assert first.hrv_baseline_7d is None
    assert first.baseline_days == 0
    assert gold[D0 + timedelta(days=3)].baseline_days == 3


def test_gold_training_load_ratio(gold):
    last = gold[D0 + timedelta(days=27)]
    assert last.acute_load_7d == 700.0  # 7 days x 100
    assert last.chronic_load_weekly_28d == 700.0  # 2800 over 4 weeks
    assert last.acute_chronic_ratio == 1.0
    # One week in, chronic load only covers 7 of 28 days, so the ratio is inflated.
    assert gold[D0 + timedelta(days=6)].acute_chronic_ratio == 4.0


def test_gold_fills_rest_days_with_zero_load(make_bronze):
    bronze = make_bronze([
        envelope("daily_summary", day(0), summary()),
        envelope("daily_summary", day(1), summary()),
        envelope("activities", day(0), [activity(1, f"{day(0)} 07:00:00", load=80)]),
    ])
    empty = make_bronze([envelope("sleep", day(0), None)])
    df = gold_daily_recovery(
        silver_daily_summary(bronze), silver_sleep(empty), silver_hrv(empty),
        silver_training_readiness(empty), silver_activities(bronze),
    )
    rows = {r.calendar_date: r for r in df.collect()}
    assert rows[D0 + timedelta(days=1)].training_load == 0
    assert rows[D0 + timedelta(days=1)].acute_load_7d == 80.0
