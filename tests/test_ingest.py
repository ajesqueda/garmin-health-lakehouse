import json
from datetime import date

from garmin_lakehouse.ingest import ENDPOINTS, date_range, ingest


class TooManyRequestsError(Exception):
    """Stand-in for garminconnect.GarminConnectTooManyRequestsError (matched by name)."""


class FakeGarmin:
    def __init__(self, fail_on: dict | None = None):
        self.calls: list[tuple[str, str]] = []
        self.fail_on = fail_on or {}

    def _record(self, name: str, cdate: str):
        self.calls.append((name, cdate))
        if (name, cdate) in self.fail_on:
            raise self.fail_on[(name, cdate)]

    def get_user_summary(self, cdate):
        self._record("daily_summary", cdate)
        return {"calendarDate": cdate, "totalSteps": 9000}

    def get_sleep_data(self, cdate):
        self._record("sleep", cdate)
        return {"dailySleepDTO": {"calendarDate": cdate, "sleepTimeSeconds": 27000}}

    def get_hrv_data(self, cdate):
        self._record("hrv", cdate)
        return None  # e.g. watch not worn overnight

    def get_training_readiness(self, cdate):
        self._record("training_readiness", cdate)
        return [{"calendarDate": cdate, "score": 70}]

    def get_activities_by_date(self, startdate, enddate=None):
        self._record("activities", startdate)
        assert enddate == startdate  # one-day ranges keep one file per day
        return []


def run(client, tmp_path, days, today, **kw):
    return ingest(client, tmp_path, days, today=today, sleep=lambda s: None, **kw)


def test_date_range_is_oldest_first_and_inclusive():
    assert date_range(date(2026, 10, 27), 3) == [
        date(2026, 10, 25), date(2026, 10, 26), date(2026, 10, 27)
    ]


def test_writes_one_envelope_per_endpoint_and_day(tmp_path):
    report = run(FakeGarmin(), tmp_path, [date(2026, 10, 20)], today=date(2026, 10, 27))
    names = sorted(p.relative_to(tmp_path).as_posix() for p in report.written)
    assert names == [
        "daily_summary/2026-10-20.json",
        "sleep/2026-10-20.json",
        "training_readiness/2026-10-20.json",
    ]
    envelope = json.loads((tmp_path / "sleep/2026-10-20.json").read_text())
    assert envelope["endpoint"] == "sleep"
    assert envelope["calendar_date"] == "2026-10-20"
    assert envelope["payload"]["dailySleepDTO"]["sleepTimeSeconds"] == 27000
    assert "fetched_at" in envelope


def test_empty_responses_leave_a_null_marker(tmp_path):
    report = run(FakeGarmin(), tmp_path, [date(2026, 10, 20)], today=date(2026, 10, 27))
    assert report.empty == 2  # hrv None, activities []
    marker = json.loads((tmp_path / "hrv/2026-10-20.json").read_text())
    assert marker["payload"] is None
    assert json.loads((tmp_path / "activities/2026-10-20.json").read_text())["payload"] is None


def test_rerun_skips_old_days_but_refetches_recent_ones(tmp_path):
    days = date_range(date(2026, 10, 27), 4)
    run(FakeGarmin(), tmp_path, days, today=date(2026, 10, 27))
    client = FakeGarmin()
    report = run(client, tmp_path, days, today=date(2026, 10, 27), overwrite_recent=2)
    fetched_days = {d for _, d in client.calls}
    assert fetched_days == {"2026-10-26", "2026-10-27"}
    assert report.skipped_existing == 2 * 5  # 2 old days x all 5 endpoints, empties too
    # The earlier version is kept; the refetch lands next to it.
    versions = sorted(p.name for p in (tmp_path / "sleep").glob("2026-10-27*.json"))
    assert len(versions) == 2 and versions[0] == "2026-10-27.json"


def test_one_failure_does_not_stop_the_run(tmp_path):
    client = FakeGarmin(fail_on={("sleep", "2026-10-20"): ConnectionError("boom")})
    report = run(client, tmp_path, date_range(date(2026, 10, 21), 2), today=date(2026, 10, 27))
    assert "sleep/2026-10-20" in report.failed
    assert (tmp_path / "sleep/2026-10-21.json").exists()


def test_rate_limit_stops_the_run_early(tmp_path):
    client = FakeGarmin(fail_on={("daily_summary", "2026-10-20"): TooManyRequestsError()})
    report = run(client, tmp_path, date_range(date(2026, 10, 21), 2), today=date(2026, 10, 27))
    assert report.failed == {"daily_summary/2026-10-20": "TooManyRequestsError"}
    assert client.calls == [("daily_summary", "2026-10-20")]


def test_no_temp_files_left_behind(tmp_path):
    run(FakeGarmin(), tmp_path, [date(2026, 10, 20)], today=date(2026, 10, 27))
    assert not list(tmp_path.rglob(".*.tmp"))


def test_every_endpoint_is_registered():
    assert set(ENDPOINTS) == {"daily_summary", "sleep", "hrv", "training_readiness", "activities"}
