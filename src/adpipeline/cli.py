"""CLI entrypoint: python -m adpipeline.cli --start 2026-08-01 --end 2026-08-07

Reads config/customers.yaml, runs the Google Ads connector against the mock
API for every google_ads ad account, converts, and upserts into Postgres
(DATABASE_URL env var). In production this is the function an Airflow
PythonOperator/task-mapped-over-accounts would call -- see DESIGN.md ยง1.
"""
from __future__ import annotations

import argparse
import logging
import os
import uuid
from datetime import date, datetime
from decimal import Decimal

from adpipeline.config import load_customers
from adpipeline.google_ads_connector import GoogleAdsConnector
from adpipeline.currency import FxRateTable
from adpipeline.postgres_writer import PostgresWriter
from adpipeline.pipeline import run_customer_extraction

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# Sample daily-close FX rates for the demo run (see DESIGN.md ยง4 for the real source: openexchangerates.org daily API).
_DEMO_FX_RATES = {
    "INR": Decimal("0.01198"),
    "GBP": Decimal("1.27"),
    "USD": Decimal("1.0"),
}


def build_demo_fx_table(start: date, end: date) -> FxRateTable:
    rates = {}
    day = start
    while day <= end:
        for currency, usd_rate in _DEMO_FX_RATES.items():
            rates[(day, currency)] = usd_rate
        day = date.fromordinal(day.toordinal() + 1)
    return FxRateTable(rates)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--config", default="config/customers.yaml")
    parser.add_argument("--today", default=None, help="Override 'today' for deterministic demo runs (YYYY-MM-DD)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    today = date.fromisoformat(args.today) if args.today else datetime.utcnow().date()

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL env var is required, e.g. postgresql://adpipeline:adpipeline@localhost:5433/adpipeline")

    import psycopg  # imported lazily so unit tests don't need a driver installed

    customers = load_customers(args.config)
    fx_table = build_demo_fx_table(start, end)
    run_id = uuid.uuid4()

    with psycopg.connect(database_url) as conn:
        writer = PostgresWriter(conn)
        customer_id_map, ad_account_id_map = _ensure_dims(conn, customers)

        for customer in customers:
            connector = GoogleAdsConnector(today=today)
            result = run_customer_extraction(
                customer=customer,
                customer_id=customer_id_map[customer.external_key],
                ad_account_ids=ad_account_id_map[customer.external_key],
                connector=connector,
                fx_table=fx_table,
                writer=writer,
                start=start,
                end=end,
                today=today,
                run_id=run_id,
            )
            logger.info(
                "customer=%s rows_written=%d accounts_failed=%s",
                customer.external_key, result.rows_written, result.accounts_failed,
            )


def _ensure_dims(conn, customers) -> tuple[dict, dict]:
    """Idempotently upsert dim_customer / dim_ad_account from the YAML config
    and return id lookup maps. In production this is a separate, rarely-run
    onboarding step -- inlined here so the demo is a single command.
    """
    customer_id_map: dict[str, int] = {}
    ad_account_id_map: dict[str, dict[str, int]] = {}

    with conn.cursor() as cur:
        for customer in customers:
            cur.execute(
                """
                INSERT INTO ads.dim_customer (external_key, display_name, reporting_timezone, reporting_currency, refresh_cadence)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (external_key) DO UPDATE SET
                    reporting_timezone = EXCLUDED.reporting_timezone,
                    reporting_currency = EXCLUDED.reporting_currency,
                    refresh_cadence = EXCLUDED.refresh_cadence,
                    updated_at = now()
                RETURNING customer_id
                """,
                (customer.external_key, customer.display_name, customer.reporting_timezone,
                 customer.reporting_currency, customer.refresh_cadence),
            )
            (customer_id,) = cur.fetchone()
            customer_id_map[customer.external_key] = customer_id
            ad_account_id_map[customer.external_key] = {}

            for account in customer.ad_accounts:
                platform_id = {"meta": 1, "google_ads": 2, "tiktok": 3}[account.platform]
                cur.execute(
                    """
                    INSERT INTO ads.dim_ad_account (customer_id, platform_id, platform_account_id, account_timezone, account_currency)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (platform_id, platform_account_id) DO UPDATE SET
                        account_timezone = EXCLUDED.account_timezone,
                        account_currency = EXCLUDED.account_currency
                    RETURNING ad_account_id
                    """,
                    (customer_id, platform_id, account.platform_account_id, account.account_timezone, account.account_currency),
                )
                (ad_account_id,) = cur.fetchone()
                ad_account_id_map[customer.external_key][account.platform_account_id] = ad_account_id
        conn.commit()

    return customer_id_map, ad_account_id_map


if __name__ == "__main__":
    main()
