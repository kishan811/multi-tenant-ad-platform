"""Tests for pipeline._to_fact_row's finality combination logic.

Google Ads gives the connector no explicit "is this row final" flag
(GoogleAdsConnector always reports is_reporting_lag_final=True -- see
google_ads_connector.py docstring), so for this platform is_final in the
fact table is determined *purely* by row age against
MAX_ATTRIBUTION_WINDOW_DAYS. This is the one place worth a dedicated test:
get the AND-combination wrong and every Google Ads row would either never
finalize (if the default flipped) or finalize immediately (defeating
reconciliation altogether).
"""
from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4

from adpipeline.config import AdAccountConfig, CustomerConfig
from adpipeline.currency import FxRateTable
from adpipeline.models import RawCampaignDayMetric
from adpipeline.pipeline import MAX_ATTRIBUTION_WINDOW_DAYS, _to_fact_row


def _customer() -> CustomerConfig:
    account = AdAccountConfig(
        platform="google_ads", platform_account_id="1112223330",
        account_timezone="America/New_York", account_currency="USD",
    )
    return CustomerConfig(
        external_key="itest", display_name="ITest", reporting_timezone="America/New_York",
        reporting_currency="USD", refresh_cadence="daily", ad_accounts=(account,),
    )


def _metric(report_date: date) -> RawCampaignDayMetric:
    return RawCampaignDayMetric(
        platform="google_ads", platform_account_id="1112223330", platform_campaign_id="6001",
        campaign_name="Test Campaign", report_date=report_date, account_timezone="America/New_York",
        account_currency="USD", spend_minor=10000, impressions=1000, clicks=100, conversions=5.0,
        is_reporting_lag_final=True,  # Google Ads connector always reports this
    )


def test_row_within_attribution_window_is_not_final():
    customer = _customer()
    today = date(2026, 8, 20)
    report_date = today - timedelta(days=MAX_ATTRIBUTION_WINDOW_DAYS - 1)  # 1 day short of the window closing
    row = _to_fact_row(_metric(report_date), customer.ad_accounts[0], customer, 1, 10,
                        FxRateTable({}), today, uuid4())
    assert row.is_final is False


def test_row_past_attribution_window_is_final():
    customer = _customer()
    today = date(2026, 8, 20)
    report_date = today - timedelta(days=MAX_ATTRIBUTION_WINDOW_DAYS)  # exactly at the window boundary
    row = _to_fact_row(_metric(report_date), customer.ad_accounts[0], customer, 1, 10,
                        FxRateTable({}), today, uuid4())
    assert row.is_final is True
