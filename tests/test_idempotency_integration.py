"""Integration test against a REAL Postgres, verifying the ON CONFLICT SQL
in postgres_writer.py behaves the same way the FakePostgresConnection double
in test_idempotency.py asserts it does.

Skipped automatically unless DATABASE_URL is set (see README "Running tests").
"""
import os
import uuid
from datetime import date
from decimal import Decimal

import pytest

pytest.importorskip("psycopg")

from adpipeline.postgres_writer import FactRow, PostgresWriter  # noqa: E402

DATABASE_URL = os.environ.get("DATABASE_URL")

pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="set DATABASE_URL to run against a live Postgres")


@pytest.fixture
def conn():
    import psycopg

    connection = psycopg.connect(DATABASE_URL)
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ads.dim_customer (external_key, display_name, reporting_timezone, reporting_currency)
            VALUES ('itest-customer', 'ITest Customer', 'America/New_York', 'USD')
            ON CONFLICT (external_key) DO UPDATE SET updated_at = now()
            RETURNING customer_id
            """
        )
        (customer_id,) = cur.fetchone()
        cur.execute(
            """
            INSERT INTO ads.dim_ad_account (customer_id, platform_id, platform_account_id, account_timezone, account_currency)
            VALUES (%s, 1, 'act_itest', 'Asia/Kolkata', 'INR')
            ON CONFLICT (platform_id, platform_account_id) DO UPDATE SET account_timezone = EXCLUDED.account_timezone
            RETURNING ad_account_id
            """,
            (customer_id,),
        )
        (ad_account_id,) = cur.fetchone()
    connection.commit()
    connection.itest_customer_id = customer_id
    connection.itest_ad_account_id = ad_account_id
    yield connection
    with connection.cursor() as cur:
        cur.execute("DELETE FROM ads.fact_campaign_performance WHERE ad_account_id = %s", (ad_account_id,))
        cur.execute("DELETE FROM ads.dim_campaign WHERE ad_account_id = %s", (ad_account_id,))
    connection.commit()
    connection.close()


def test_rerunning_same_batch_against_real_postgres_does_not_duplicate(conn):
    writer = PostgresWriter(conn)
    row = FactRow(
        customer_id=conn.itest_customer_id,
        ad_account_id=conn.itest_ad_account_id,
        platform_id=1,
        platform_campaign_id="itest-6001",
        campaign_name="ITest Campaign",
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
        run_id=uuid.uuid4(),
    )

    writer.upsert_campaign_daily([row])
    writer.upsert_campaign_daily([row])

    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM ads.fact_campaign_performance WHERE ad_account_id = %s",
            (conn.itest_ad_account_id,),
        )
        (count,) = cur.fetchone()
    assert count == 1
