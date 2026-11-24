"""Synthetic history only: no real health data in the repo."""

import json
import subprocess
from datetime import date, timedelta

import numpy as np
import pytest

from garmin_lakehouse import health_markov as hm
from garmin_lakehouse.health_markov import (
    STATES,
    Day,
    classify,
    fit,
    long_run,
    main,
    predict,
    transitions,
)

START = date(2026, 1, 1)


def history(states, sleep_cut=7.0, hrv_cut=50.0, start=START):
    """Days whose (sleep, HRV) land in the given state indexes."""
    values = {0: (8.0, 60.0), 1: (6.0, 60.0), 2: (8.0, 40.0), 3: (6.0, 40.0)}
    return [Day(start + timedelta(days=i), *values[s]) for i, s in enumerate(states)]


# --- states ----------------------------------------------------------------------------

@pytest.mark.parametrize("sleep, hrv, expected", [
    (8, 60, "well rested"),
    (6, 60, "short sleep, HRV ok"),
    (8, 40, "slept well, HRV low"),
    (6, 40, "run down"),
    (7, 50, "well rested"),  # exactly on both cuts counts as the better side
])
def test_classify(sleep, hrv, expected):
    assert STATES[classify(sleep, hrv, 7.0, 50.0)] == expected


# --- transitions -----------------------------------------------------------------------

def test_transitions_pair_consecutive_days():
    assert transitions(history([0, 1, 3]), 7.0, 50.0) == [(0, 1), (1, 3)]


def test_a_missing_day_breaks_the_chain():
    days = history([0, 1]) + history([3, 3], start=START + timedelta(days=3))
    assert transitions(days, 7.0, 50.0) == [(0, 1), (3, 3)]  # no (1, 3) across the gap


def test_a_day_without_hrv_breaks_the_chain():
    days = history([0, 0, 0])
    days[1] = Day(days[1].day, 8.0, None)
    assert transitions(days, 7.0, 50.0) == []


def test_order_of_rows_doesnt_matter():
    days = history([0, 2, 3, 1])
    assert transitions(list(reversed(days)), 7.0, 50.0) == transitions(days, 7.0, 50.0)


# --- fitting -------------------------------------------------------------------------------

def test_columns_are_probabilities_and_nothing_is_impossible():
    model = fit(history([0, 0, 1, 0, 2, 3, 3, 0]), hrv_cut=50.0)
    assert np.allclose(model.P.sum(axis=0), 1)
    assert (model.P > 0).all()  # smoothing: unseen moves get a small chance


def test_counts_are_to_from():
    model = fit(history([0, 1, 1]), hrv_cut=50.0)
    assert model.counts[1, 0] == 1  # 0 -> 1
    assert model.counts[1, 1] == 1  # 1 -> 1
    assert model.days_used == 2


def test_without_smoothing_it_is_the_raw_frequencies():
    model = fit(history([0, 1, 0, 0, 0, 1]), hrv_cut=50.0, smoothing=0)
    # from "well rested": 0->1 twice, 0->0 twice
    assert model.P[:, 0].tolist() == [0.5, 0.5, 0, 0]


def test_unseen_state_without_smoothing_is_uniform_not_nan():
    model = fit(history([0, 0, 0]), hrv_cut=50.0, smoothing=0)
    assert np.allclose(model.P[:, 3], 0.25)
    assert np.isfinite(model.P).all()


def test_hrv_cut_defaults_to_the_median():
    days = [Day(START + timedelta(days=i), 8.0, h) for i, h in enumerate([30, 40, 50, 60, 70])]
    assert fit(days).hrv_cut == 50


def test_no_hrv_at_all_is_an_error():
    with pytest.raises(ValueError, match="no HRV"):
        fit([Day(START, 8.0, None)])


# --- prediction --------------------------------------------------------------------------

def test_predict_one_day_is_the_column():
    model = fit(history([0, 1, 1, 3, 0, 2]), hrv_cut=50.0)
    assert np.allclose(predict(model, 1, 1), model.P[:, 1])


def test_predict_k_days_is_the_matrix_power():
    model = fit(history([0, 1, 1, 3, 0, 2, 2, 0]), hrv_cut=50.0)
    expected = np.linalg.matrix_power(model.P, 3)[:, 2]
    assert np.allclose(predict(model, 2, 3), expected)
    assert predict(model, 2, 3).sum() == pytest.approx(1)


def test_long_run_is_steady_and_reached_from_anywhere():
    rng = np.random.default_rng(7)
    model = fit(history(rng.integers(0, 4, size=60).tolist()), hrv_cut=50.0)
    s = long_run(model)
    assert np.allclose(model.P @ s, s)
    assert s.sum() == pytest.approx(1)
    for state in range(4):
        assert np.allclose(predict(model, state, 80), s, atol=1e-8)


def test_a_sticky_history_predicts_staying_put():
    model = fit(history([3] * 30 + [0] * 30), hrv_cut=50.0)
    assert np.argmax(predict(model, 3, 1)) == 3
    assert np.argmax(predict(model, 0, 1)) == 0


# --- CLI ----------------------------------------------------------------------------------

def fake_loader(states):
    def load(table, profile, warehouse):
        return history(states)
    return load


def test_cli_end_to_end(capsys):
    code = main(["--sleep", "6", "--hrv", "40", "--days", "2"],
                loader=fake_loader([0, 1, 3, 3, 0, 2, 3, 1, 0, 0]))
    out = capsys.readouterr().out
    assert code == 0
    assert "-> run down" in out
    assert "In 2 days:" in out
    assert "Long run, share of days:" in out
    assert "Few examples from this state" in out


def test_cli_reports_a_failed_load(capsys):
    def broken(table, profile, warehouse):
        raise RuntimeError("Databricks login expired: run `databricks auth login --profile free`")
    assert main(["--sleep", "7", "--hrv", "50"], loader=broken) == 1
    assert "auth login" in capsys.readouterr().err


@pytest.mark.parametrize("args", [["--sleep", "7", "--hrv", "50", "--days", "0"],
                                  ["--sleep", "-1", "--hrv", "50"],
                                  ["--sleep", "nan", "--hrv", "50"],
                                  ["--sleep", "7", "--hrv", "inf"],
                                  ["--sleep", "7", "--hrv", "50", "--hrv-cut", "nan"]])
def test_cli_rejects_bad_input(args):
    with pytest.raises(SystemExit):
        main(args, loader=fake_loader([0, 0]))


# --- Databricks loader (CLI calls faked) ----------------------------------------------------

class FakeRun:
    def __init__(self, stdout="", stderr="", returncode=0):
        self.stdout, self.stderr, self.returncode = stdout, stderr, returncode


def test_loader_parses_rows_and_finds_the_warehouse(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[1] == "warehouses":
            return FakeRun(json.dumps([{"id": "wh1"}]))
        body = json.loads(cmd[cmd.index("--json") + 1])
        assert body["warehouse_id"] == "wh1"
        return FakeRun(json.dumps({"status": {"state": "SUCCEEDED"}, "result": {
            "data_array": [["2026-01-01", "7.5", "52"], ["2026-01-02", None, "48"]]}}))

    monkeypatch.setattr(subprocess, "run", fake_run)
    days = hm.load_from_databricks("t", "free", None)
    assert days == [Day(date(2026, 1, 1), 7.5, 52.0), Day(date(2026, 1, 2), None, 48.0)]
    assert calls[0][:3] == ["databricks", "warehouses", "list"]


def test_loader_explains_an_expired_login(monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: FakeRun(
        stderr='{"error":"invalid_grant","error_description":"Refresh token is invalid"}',
        returncode=1))
    with pytest.raises(RuntimeError, match="databricks auth login --profile free"):
        hm.load_from_databricks("t", "free", "wh1")


def test_negative_smoothing_is_rejected():
    with pytest.raises(ValueError, match="negative"):
        fit(history([0, 1, 2, 3]), smoothing=-1)


def test_report_says_where_the_hrv_cut_came_from(capsys):
    main(["--sleep", "8", "--hrv", "60"], loader=fake_loader([0, 1, 3, 0]))
    assert "= your median" in capsys.readouterr().out
    main(["--sleep", "8", "--hrv", "60", "--hrv-cut", "45"], loader=fake_loader([0, 1, 3, 0]))
    out = capsys.readouterr().out
    assert "from --hrv-cut" in out and "your median" not in out


class FakeApi:
    """Answers `databricks api ...` calls from a script of responses."""

    def __init__(self, post, gets):
        self.post, self.gets, self.paths = post, dict(gets), []

    def __call__(self, cmd, **kwargs):
        assert kwargs.get("timeout"), "every CLI call needs a timeout"
        if cmd[2] == "post":
            return FakeRun(json.dumps(self.post))
        self.paths.append(cmd[3])
        return FakeRun(json.dumps(self.gets[cmd[3]]))


def test_loader_polls_a_slow_query_and_reads_every_chunk(monkeypatch):
    base = "/api/2.0/sql/statements/s1"
    api = FakeApi(post={"statement_id": "s1", "status": {"state": "PENDING"}}, gets={
        base: {"statement_id": "s1", "status": {"state": "SUCCEEDED"}, "result": {
            "data_array": [["2026-01-01", "7.5", "52"]], "next_chunk_index": 1}},
        f"{base}/result/chunks/1": {"data_array": [["2026-01-02", "6", "48"]],
                                    "next_chunk_index": 2},
        f"{base}/result/chunks/2": {"data_array": [["2026-01-03", "8", "55"]]},
    })
    monkeypatch.setattr(subprocess, "run", api)
    naps = []
    days = hm.load_from_databricks("t", "free", "wh1", sleep=naps.append)
    assert [d.day.day for d in days] == [1, 2, 3]
    assert naps == [5]
    assert api.paths == [base, f"{base}/result/chunks/1", f"{base}/result/chunks/2"]


def test_loader_gives_up_on_a_query_that_never_finishes(monkeypatch):
    running = {"statement_id": "s1", "status": {"state": "RUNNING"}}
    monkeypatch.setattr(subprocess, "run", FakeApi(
        post=running, gets={"/api/2.0/sql/statements/s1": running}))
    ticks = iter(range(0, 10_000, 100))
    with pytest.raises(RuntimeError, match="still running"):
        hm.load_from_databricks("t", "free", "wh1", sleep=lambda s: None,
                                clock=lambda: next(ticks))


@pytest.mark.parametrize("table", ["t; DROP TABLE x", "a.b.c.d", "gold daily", ""])
def test_loader_rejects_anything_but_a_table_name(table):
    with pytest.raises(ValueError, match="not a table name"):
        hm.load_from_databricks(table, "free", "wh1")


@pytest.mark.parametrize("error, message", [
    (FileNotFoundError("databricks"), "isn't installed"),
    (subprocess.TimeoutExpired("databricks", 120), "didn't answer"),
])
def test_cli_problems_become_clear_errors(monkeypatch, error, message):
    def broken(*a, **k):
        raise error
    monkeypatch.setattr(subprocess, "run", broken)
    with pytest.raises(RuntimeError, match=message):
        hm.load_from_databricks("t", "free", "wh1")
