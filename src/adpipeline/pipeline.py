"""Orchestrates one customer's extraction: connector -> normalize -> tz bucket
-> currency convert -> idempotent write.

This is the piece a real scheduler (Airflow -- see DESIGN.md ยง1) would call
once per (ad_account, date_range) task instance -- one Airflow task per
dynamically-mapped ad account, per the DAG design in DESIGN.md ยง1.2.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from uuid import UUID

from adpipeline.config import AdAccountConfig, CustomerConfig
from adpipeline.base_connector import AuthError, BaseConnector
from adpipeline.currency import FxRateTable, convert_minor_units
from adpipeline.postgres_writer import FactRow, PostgresWriter
from adpipeline.timezone_utils import account_day_to_reporting_day

logger = logging.getLogger(__name__)

MAX_ATTRIBUTION_WINDOW_DAYS = 30  # Google Ads' default UI reporting/conversion-lag window; make configurable per platform in prod

# Toy platform_id / customer_id / ad_account_id resolution for the take-home.
# In production these come from dim_platform / dim_customer / dim_ad_account
# (looked up or upserted during customer onboarding), not hardcoded.
PLATFORM_IDS = {"meta": 1, "google_ads": 2, "tiktok": 3}


@dataclass
class PipelineRunResult:
    rows_written: int
    accounts_failed: list[str]


def run_customer_extraction(
    customer: CustomerConfig,
    customer_id: int,
    ad_account_ids: dict[str, int],  # platform_account_id -> dim_ad_account.ad_account_id
    connector: BaseConnector,
    fx_table: FxRateTable,
    writer: PostgresWriter,
    start: date,
    end: date,
    today: date,
    run_id: UUID,
) -> PipelineRunResult:
    rows_written = 0
    accounts_failed: list[str] = []

    for account in customer.ad_accounts:
        if account.platform != connector.platform:
            continue
        try:
            result = connector.extract(account, start, end)
        except AuthError as exc:
            logger.error(
                "auth failure customer=%s account=%s: %s -- flag token_status=expired/revoked, alert on-call",
                customer.external_key, account.platform_account_id, exc,
            )
            accounts_failed.append(account.platform_account_id)
            continue

        fact_rows = [
            _to_fact_row(metric, account, customer, customer_id,
                         ad_account_ids[account.platform_account_id], fx_table, today, run_id)
            for metric in result.metrics
        ]
        rows_written += writer.upsert_campaign_daily(fact_rows)

    return PipelineRunResult(rows_written=rows_written, accounts_failed=accounts_failed)


def _to_fact_row(metric, account: AdAccountConfig, customer: CustomerConfig, customer_id: int,
                  ad_account_id: int, fx_table: FxRateTable, today: date, run_id: UUID) -> FactRow:
    reporting_day = account_day_to_reporting_day(
        metric.report_date, account.account_timezone, customer.reporting_timezone
    )
    fx_rate_date = min(metric.report_date, today)  # never look up a future FX rate
    converted_minor, fx_rate = convert_minor_units(
        metric.spend_minor, account.account_currency, customer.reporting_currency, fx_rate_date, fx_table
    )

    days_old = (today - metric.report_date).days
    is_final = metric.is_reporting_lag_final and days_old >= MAX_ATTRIBUTION_WINDOW_DAYS

    return FactRow(
        customer_id=customer_id,
        ad_account_id=ad_account_id,
        platform_id=PLATFORM_IDS[account.platform],
        platform_campaign_id=metric.platform_campaign_id,
        campaign_name=metric.campaign_name,
        reporting_day=reporting_day,
        spend_minor_source=metric.spend_minor,
        source_currency=account.account_currency,
        spend_minor_reporting=converted_minor,
        reporting_currency=customer.reporting_currency,
        fx_rate_used=Decimal(str(fx_rate)),
        fx_rate_date=fx_rate_date,
        impressions=metric.impressions,
        clicks=metric.clicks,
        conversions=metric.conversions,
        attribution_window_days=MAX_ATTRIBUTION_WINDOW_DAYS,
        is_final=is_final,
        run_id=run_id,
    )
