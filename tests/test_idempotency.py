from datetime import date
from decimal import Decimal
from uuid import uuid4

from adpipeline.postgres_writer import FactRow, PostgresWriter
from tests.fakes import FakePostgresConnection


def _row(**overrides) -> FactRow:
    base = dict(
        customer_id=1,
        ad_account_id=10,
        platform_id=1,
        platform_campaign_id="6001",
        campaign_name="Acme - Prospecting - IN",
        reporting_day=date(2026, 8, 15),
        spend_minor_source=145075,
        source_currency="INR",
        spend_minor_reporting=1738,
        reporting_currency="USD",
        fx_rate_used=Decimal("0.01198"),
        fx_rate_date=date(2026, 8, 15),
        impressions=82000,
        clicks=1890,
        conversions=42.0,
        attribution_window_days=7,
        is_final=True,
        run_id=uuid4(),
    )
    base.update(overrides)
    return FactRow(**base)


def test_rerunning_the_same_batch_does_not_duplicate_rows():
    conn = FakePostgresConnection()
    writer = PostgresWriter(conn)

    writer.upsert_campaign_daily([_row()])
    writer.upsert_campaign_daily([_row()])  # simulate a crashed-worker retry re-sending the same batch

    assert len(conn.facts) == 1


def test_rerun_with_revised_conversions_updates_in_place():
    # Simulates attribution-window revision: same (account, campaign, day),
    # conversions increase on a later pull.
    conn = FakePostgresConnection()
    writer = PostgresWriter(conn)

    writer.upsert_campaign_daily([_row(conversions=30.0, is_final=False)])
    writer.upsert_campaign_daily([_row(conversions=42.0, is_final=True)])

    assert len(conn.facts) == 1
    (only_row,) = conn.facts.values()
    assert only_row["conversions"] == 42.0
    assert only_row["is_final"] is True


def test_different_reporting_days_do_not_collide():
    conn = FakePostgresConnection()
    writer = PostgresWriter(conn)

    writer.upsert_campaign_daily([_row(reporting_day=date(2026, 8, 15))])
    writer.upsert_campaign_daily([_row(reporting_day=date(2026, 8, 16))])

    assert len(conn.facts) == 2


def test_different_campaigns_on_same_account_do_not_collide():
    conn = FakePostgresConnection()
    writer = PostgresWriter(conn)

    writer.upsert_campaign_daily([_row(platform_campaign_id="6001")])
    writer.upsert_campaign_daily([_row(platform_campaign_id="6002")])

    assert len(conn.facts) == 2


def test_same_campaign_different_ad_accounts_do_not_collide():
    # Two ad accounts can legitimately have campaigns with the same platform_campaign_id
    # only if the platform reuses IDs across accounts -- not true for Google Ads (campaign
    # IDs are unique per customer account), but the natural key includes ad_account_id
    # specifically so this can never cause a silent merge even if that assumption is wrong.
    conn = FakePostgresConnection()
    writer = PostgresWriter(conn)

    writer.upsert_campaign_daily([_row(ad_account_id=10, platform_campaign_id="6001")])
    writer.upsert_campaign_daily([_row(ad_account_id=11, platform_campaign_id="6001")])

    assert len(conn.facts) == 2


def test_empty_batch_is_a_noop():
    conn = FakePostgresConnection()
    writer = PostgresWriter(conn)
    written = writer.upsert_campaign_daily([])
    assert written == 0
    assert conn.facts == {}
