"""An in-memory double that mimics *just enough* Postgres upsert semantics
(ON CONFLICT ... DO UPDATE, keyed on a unique constraint) to unit-test
PostgresWriter's idempotency contract without a live database.

This intentionally re-derives the same conflict-target logic as the real SQL
so that "run the same batch twice -> same end state" can be asserted in pure
Python. The real behavior is additionally verified against a live Postgres
in tests/test_idempotency_integration.py (see README for how to run it).
"""
from __future__ import annotations


class _FakeCursor:
    def __init__(self, store):
        self.store = store
        self._last_result = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params):
        sql_norm = " ".join(sql.split())
        if sql_norm.startswith("INSERT INTO ads.dim_campaign"):
            key = (params["ad_account_id"], params["platform_campaign_id"])
            campaigns = self.store.setdefault("campaigns", {})
            if key not in campaigns:
                campaigns[key] = len(campaigns) + 1
            campaign_sk = campaigns[key]
            self.store.setdefault("campaign_names", {})[key] = params["campaign_name"]
            self._last_result = (campaign_sk,)
        elif sql_norm.startswith("INSERT INTO ads.fact_campaign_performance"):
            key = (params["ad_account_id"], params["campaign_sk"], params["reporting_day"])
            facts = self.store.setdefault("facts", {})
            existing = facts.get(key)
            changed = existing is None or (
                existing["spend_minor_source"] != params["spend_minor_source"]
                or existing["conversions"] != params["conversions"]
                or existing["is_final"] != params["is_final"]
            )
            if changed:
                facts[key] = dict(params)
            self._last_result = None
        else:
            raise AssertionError(f"unexpected SQL in fake: {sql_norm[:60]}")

    def fetchone(self):
        return self._last_result


class FakePostgresConnection:
    """Fakes just the subset of the psycopg connection API PostgresWriter uses."""

    def __init__(self):
        self.store: dict = {}

    def cursor(self):
        return _FakeCursor(self.store)

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    @property
    def facts(self) -> dict:
        return self.store.get("facts", {})
