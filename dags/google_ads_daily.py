"""Illustrative Airflow DAG for the Google Ads daily extraction, matching the
design in DESIGN.md Section 1. This is NOT wired up or tested against a real
Airflow environment (that would need real connections, pools, and a scheduler
to verify against) -- it exists to make the orchestration design concrete as
code rather than only prose, and to show the actual Airflow API being used
(TaskFlow, dynamic task mapping) rather than just describing it.

The real work per mapped task -- fetch config, extract, normalize, tz-bucket,
currency-convert, idempotent upsert -- is exactly what cli.py / pipeline.py
already do end-to-end; this DAG's job is only to discover accounts and call
that same code path once per account, on a schedule, with Airflow's retry/
backoff/pool machinery around it instead of a single manual `python -m` run.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

# See DESIGN.md Section 5.1: extract yesterday plus a trailing reconciliation
# window sized to the platform's own conversion-lag window (30 days for
# Google Ads -- see pipeline.MAX_ATTRIBUTION_WINDOW_DAYS).
RECONCILIATION_WINDOW_DAYS = 30


@dag(
    dag_id="google_ads_daily",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 5,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
    tags=["ads-pipeline", "google_ads"],
)
def google_ads_daily():
    @task
    def get_active_accounts() -> list[dict]:
        """Discover active google_ads ad accounts from dim_ad_account.

        Real implementation: SELECT ad_account_id, platform_account_id,
        customer_id FROM ads.dim_ad_account WHERE platform_id = 2 AND
        is_active AND token_status = 'ok'. Returning a static list here since
        this DAG isn't run against a live Airflow + Postgres pair.
        """
        return [
            {"customer_external_key": "acme-d2c", "platform_account_id": "1112223330"},
            {"customer_external_key": "glow-cosmetics", "platform_account_id": "2223334440"},
            {"customer_external_key": "glow-cosmetics", "platform_account_id": "2223334441"},
        ]

    @task(
        pool="google_ads_api",  # per-platform pool bounds total concurrent API calls (Section 6.1)
        retries=5,
    )
    def extract_normalize_load(account: dict, data_interval_end=None) -> dict:
        """One mapped task instance = one (platform, ad_account) extraction.

        This calls the exact same run_customer_extraction() used by cli.py --
        the DAG task is a thin scheduling wrapper around code already proven
        to work standalone, not a second implementation of the pipeline.
        """
        import uuid
        from datetime import date, timedelta as td

        from adpipeline.config import load_customers
        from adpipeline.currency import FxRateTable
        from adpipeline.google_ads_connector import GoogleAdsConnector
        from adpipeline.pipeline import run_customer_extraction
        from adpipeline.postgres_writer import PostgresWriter

        today = data_interval_end.date() if data_interval_end else date.today()
        start = today - td(days=RECONCILIATION_WINDOW_DAYS)

        customers = {c.external_key: c for c in load_customers("config/customers.yaml")}
        customer = customers[account["customer_external_key"]]

        # In production: DATABASE_URL / connection pulled from an Airflow
        # Connection, not an env var; FX table read from ads.dim_fx_rate,
        # not hardcoded -- both are the same simplifications cli.py makes,
        # kept consistent here rather than diverging between the two entrypoints.
        import os

        import psycopg

        with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
            writer = PostgresWriter(conn)
            connector = GoogleAdsConnector(today=today)
            result = run_customer_extraction(
                customer=customer,
                customer_id=1,  # looked up from dim_customer in production
                ad_account_ids={account["platform_account_id"]: 1},  # looked up from dim_ad_account
                connector=connector,
                fx_table=FxRateTable({}),  # loaded from ads.dim_fx_rate in production
                writer=writer,
                start=start,
                end=today,
                today=today,
                run_id=uuid.uuid4(),
            )
        return {"account": account["platform_account_id"], "rows_written": result.rows_written}

    extract_normalize_load.expand(account=get_active_accounts())


google_ads_daily()
