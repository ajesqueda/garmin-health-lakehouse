"""Garmin rotates the refresh token on every refresh, so the job must save the
tokens it ends up with. These tests use a fake login that rotates like Garmin."""

import json

import pytest

from garmin_lakehouse.ingest import login_with_token_store, main, save_tokens


class FakeSession:
    def __init__(self, tokens: dict):
        self.tokens = tokens

    def dumps(self) -> str:
        return json.dumps(self.tokens)


class FakeClient:
    def __init__(self, tokens: dict):
        self.client = FakeSession(tokens)


class FakeGarmin:
    """Accepts each refresh token once and hands back a new one, like Garmin."""

    def __init__(self, valid: str):
        self.valid = {valid}
        self.issued = 0
        self.calls: list[str] = []

    def login(self, tokens: str) -> FakeClient:
        refresh = json.loads(tokens)["di_refresh_token"]
        self.calls.append(refresh)
        if refresh not in self.valid:
            raise PermissionError("Failed to retrieve social profile")
        self.valid.discard(refresh)
        self.issued += 1
        new = f"r{self.issued}"
        self.valid.add(new)
        return FakeClient({"di_token": f"a{self.issued}", "di_refresh_token": new,
                           "di_client_id": "c"})


def secret(refresh: str) -> str:
    return json.dumps({"di_token": "a0", "di_refresh_token": refresh, "di_client_id": "c"})


def stored_refresh(store) -> str:
    return json.loads(store.read_text())["di_refresh_token"]


def test_first_run_seeds_the_store_from_the_secret(tmp_path):
    store = tmp_path / "auth" / "garmin_tokens.json"
    garmin = FakeGarmin("r0")
    _, source = login_with_token_store(store, secret("r0"), login=garmin.login)
    assert source == "fallback"
    assert stored_refresh(store) == "r1"  # the rotated token, not the secret's


def test_daily_runs_keep_working_from_the_store(tmp_path):
    store = tmp_path / "garmin_tokens.json"
    garmin = FakeGarmin("r0")
    for day in range(1, 8):
        _, source = login_with_token_store(store, secret("r0"), login=garmin.login)
        assert source == ("fallback" if day == 1 else "store")
        assert stored_refresh(store) == f"r{day}"


def test_without_a_store_the_secret_dies_after_one_rotation(tmp_path):
    # What the job did before: log in from the secret every day, never save.
    garmin = FakeGarmin("r0")
    garmin.login(secret("r0"))
    with pytest.raises(PermissionError, match="social profile"):
        garmin.login(secret("r0"))


def test_stale_store_falls_back_to_a_regenerated_secret(tmp_path):
    store = tmp_path / "garmin_tokens.json"
    store.write_text(secret("expired"))
    garmin = FakeGarmin("fresh")  # the user just ran create_garmin_tokens.py
    _, source = login_with_token_store(store, secret("fresh"), login=garmin.login)
    assert source == "fallback"
    assert garmin.calls == ["expired", "fresh"]  # store tried first
    assert stored_refresh(store) == "r1"


def test_everything_stale_raises_with_both_reasons(tmp_path):
    store = tmp_path / "garmin_tokens.json"
    store.write_text(secret("old1"))
    garmin = FakeGarmin("never-used")
    with pytest.raises(RuntimeError, match="regenerate") as err:
        login_with_token_store(store, secret("old2"), login=garmin.login)
    assert "store: PermissionError" in str(err.value)
    assert "fallback: PermissionError" in str(err.value)
    assert stored_refresh(store) == "old1"  # a failed login doesn't overwrite anything


def test_no_tokens_at_all(tmp_path):
    with pytest.raises(RuntimeError, match="no Garmin tokens"):
        login_with_token_store(tmp_path / "missing.json", None, login=FakeGarmin("x").login)


def test_save_tokens_writes_the_clients_current_tokens(tmp_path):
    store = tmp_path / "nested" / "garmin_tokens.json"
    save_tokens(FakeClient({"di_refresh_token": "r9"}), store)
    assert stored_refresh(store) == "r9"
    assert not list(store.parent.glob(".*.tmp"))  # temp file renamed away


def test_cli_requires_some_tokens(tmp_path, monkeypatch):
    monkeypatch.delenv("GARMINTOKENS", raising=False)
    with pytest.raises(SystemExit):
        main(["--landing", str(tmp_path), "--token-store", str(tmp_path / "none.json")])
