import importlib.util
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "ensure_daily_run.py"
spec = importlib.util.spec_from_file_location("ensure_daily_run", SCRIPT)
edr = importlib.util.module_from_spec(spec)
spec.loader.exec_module(edr)

CHICAGO = ZoneInfo("America/Chicago")
NOW = datetime(2026, 11, 20, 9, 7, tzinfo=CHICAGO)


def run(run_id, trigger="PERIODIC", life="TERMINATED", result=None):
    state = {"life_cycle_state": life}
    if result:
        state["result_state"] = result
    return {"run_id": run_id, "trigger": trigger, "state": state}


class FakeCli:
    def __init__(self, runs, jobs=None):
        self.runs = runs
        self.jobs = jobs if jobs is not None else [{"job_id": 42, "settings": {}}]
        self.calls = []

    def __call__(self, args):
        self.calls.append(args)
        if args[:2] == ["jobs", "list"]:
            return {"jobs": self.jobs}
        if args[:2] == ["jobs", "list-runs"]:
            return {"runs": self.runs}
        if args[:2] == ["jobs", "run-now"]:
            assert json.loads(args[args.index("--json") + 1]) == {"job_id": 42}
            return {"run_id": 999}
        raise AssertionError(args)

    def started(self):
        return any(c[:2] == ["jobs", "run-now"] for c in self.calls)


@pytest.mark.parametrize("runs, action", [
    ([run(1, result="SUCCESS")], "ok"),
    ([run(1, life="RUNNING")], "ok"),
    ([run(1, life="PENDING")], "ok"),
    ([], "start"),
    ([run(1, result="FAILED")], "start"),
    ([run(2, trigger="ONE_TIME", result="FAILED"), run(1, result="FAILED")], "leave"),
    ([run(2, trigger="ONE_TIME", result="SUCCESS")], "ok"),
])
def test_decide(runs, action):
    assert edr.decide(runs)[0] == action


def test_starts_the_job_when_the_scheduler_skipped_today():
    cli = FakeCli(runs=[])
    action, reason = edr.ensure(cli, "garmin-daily", NOW)
    assert action == "start" and cli.started()
    assert "started run 999" in reason


def test_does_nothing_when_todays_run_succeeded():
    cli = FakeCli(runs=[run(1, result="SUCCESS")])
    assert edr.ensure(cli, "garmin-daily", NOW)[0] == "ok"
    assert not cli.started()


def test_dry_run_never_starts():
    cli = FakeCli(runs=[])
    action, reason = edr.ensure(cli, "garmin-daily", NOW, dry_run=True)
    assert action == "start" and not cli.started()
    assert "dry run" in reason


def test_only_asks_for_runs_since_local_midnight():
    cli = FakeCli(runs=[run(1, result="SUCCESS")])
    edr.ensure(cli, "garmin-daily", NOW)
    list_runs = next(c for c in cli.calls if c[:2] == ["jobs", "list-runs"])
    since = int(list_runs[list_runs.index("--start-time-from") + 1])
    assert datetime.fromtimestamp(since / 1000, CHICAGO) == datetime(2026, 11, 20, tzinfo=CHICAGO)


def test_missing_or_duplicate_job_is_an_error():
    with pytest.raises(RuntimeError, match="found 0"):
        edr.ensure(FakeCli(runs=[], jobs=[]), "garmin-daily", NOW)
    with pytest.raises(RuntimeError, match="found 2"):
        edr.ensure(FakeCli(runs=[], jobs=[{"job_id": 1}, {"job_id": 2}]), "garmin-daily", NOW)


def test_main_prints_one_log_line_and_exit_codes(capsys):
    def factory(cli):
        return lambda profile: cli

    def clock(tz):
        return NOW

    assert edr.main([], factory(FakeCli(runs=[])), clock) == 0
    assert edr.main([], factory(FakeCli(runs=[run(1, result="SUCCESS")])), clock) == 0
    leave = FakeCli(runs=[run(2, trigger="ONE_TIME", result="FAILED")])
    assert edr.main([], factory(leave), clock) == 2
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines[0].startswith("2026-11-20 09:07 CST START")
    assert " OK " in lines[1]
    assert " LEAVE " in lines[2]


def test_cli_errors_become_an_error_line(capsys):
    def broken(args):
        raise RuntimeError("Error: invalid access token")

    assert edr.main([], lambda profile: broken, lambda tz: NOW) == 1
    assert "ERROR Error: invalid access token" in capsys.readouterr().out
