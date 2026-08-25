from datetime import date

import pytest

from adpipeline.config import AdAccountConfig
from adpipeline.base_connector import AuthError
from adpipeline.google_ads_connector import GoogleAdsConnector


def _account(account_id: str, tz="Asia/Kolkata", currency="INR") -> AdAccountConfig:
    return AdAccountConfig(platform="google_ads", platform_account_id=account_id, account_timezone=tz, account_currency=currency)


def test_pagination_drains_all_pages():
    # 1112223330 has 2 campaigns, PAGE_SIZE=1 in the mock -> must take >=2 pages.
    connector = GoogleAdsConnector(today=date(2026, 8, 20), sleep_fn=lambda s: None)
    result = connector.extract(_account("1112223330"), date(2026, 8, 15), date(2026, 8, 15))

    campaign_ids = {m.platform_campaign_id for m in result.metrics}
    assert campaign_ids == {"6001", "6002"}
    assert result.pages_fetched >= 2


def test_rate_limit_is_retried_transparently():
    # The mock rate-limits every 3rd call per account with RESOURCE_EXHAUSTED and
    # no retry-after hint (unlike Meta's 613 error) -> connector must fall back
    # to blind exponential backoff and still succeed.
    connector = GoogleAdsConnector(today=date(2026, 8, 20), sleep_fn=lambda s: None)
    slept = []
    connector._sleep = lambda s: slept.append(s)

    # 2223334440 has 1 campaign but we call extract multiple times to accumulate
    # call_count on the shared mock client until the 3rd-call rate limit fires.
    account = _account("2223334440", tz="America/Los_Angeles", currency="USD")
    connector.extract(account, date(2026, 8, 15), date(2026, 8, 15))
    connector.extract(account, date(2026, 8, 15), date(2026, 8, 15))
    result = connector.extract(account, date(2026, 8, 15), date(2026, 8, 15))  # 3rd call -> rate limited once

    assert result.retries_used >= 1
    assert len(slept) >= 1


def test_retry_gives_up_after_max_retries():
    connector = GoogleAdsConnector(today=date(2026, 8, 20), sleep_fn=lambda s: None)
    connector.max_retries = 0

    call_count = {"n": 0}

    def always_rate_limited(customer_id, start, end, page_token, today):
        call_count["n"] += 1
        from adpipeline.mock_google_ads_api import MockGoogleAdsError
        raise MockGoogleAdsError("RESOURCE_EXHAUSTED", "Too many requests.")

    connector.client.search = always_rate_limited

    from adpipeline.base_connector import RateLimitedError
    with pytest.raises(RateLimitedError):
        connector.extract(_account("1112223330"), date(2026, 8, 15), date(2026, 8, 15))
    assert call_count["n"] == 1  # max_retries=0 -> exactly one attempt, no retry


def test_revoked_access_raises_auth_error_not_retryable():
    connector = GoogleAdsConnector(today=date(2026, 8, 20), sleep_fn=lambda s: None)
    with pytest.raises(AuthError, match="revoked"):
        connector.extract(_account("9998887770"), date(2026, 8, 15), date(2026, 8, 15))


def test_expired_oauth_token_raises_auth_error():
    connector = GoogleAdsConnector(today=date(2026, 8, 20), sleep_fn=lambda s: None)
    with pytest.raises(AuthError, match="expired"):
        connector.extract(_account("9998887771"), date(2026, 8, 15), date(2026, 8, 15))


def test_connector_never_vetoes_finality_google_ads_has_no_such_signal():
    # Google Ads gives the connector no explicit finality flag (contrast with
    # Meta's date_stop_is_final). GoogleAdsConnector always reports True (no
    # veto) regardless of row age -- finality is entirely the pipeline's job
    # (see test_reporting_lag / _to_fact_row's AND-with-age logic).
    connector = GoogleAdsConnector(today=date(2026, 8, 16), sleep_fn=lambda s: None)
    result = connector.extract(_account("1112223330"), date(2026, 8, 15), date(2026, 8, 15))
    assert all(m.is_reporting_lag_final for m in result.metrics)


def test_conversions_revise_upward_as_attribution_window_matures():
    account = _account("1112223330")
    early = GoogleAdsConnector(today=date(2026, 8, 16), sleep_fn=lambda s: None).extract(
        account, date(2026, 8, 15), date(2026, 8, 15)
    )
    late = GoogleAdsConnector(today=date(2026, 9, 20), sleep_fn=lambda s: None).extract(  # past the 30-day lag window
        account, date(2026, 8, 15), date(2026, 8, 15)
    )
    early_conv = sum(m.conversions for m in early.metrics if m.platform_campaign_id == "6001")
    late_conv = sum(m.conversions for m in late.metrics if m.platform_campaign_id == "6001")
    assert late_conv > early_conv
    assert late_conv == pytest.approx(42.0)  # fully matured


def test_cost_micros_parsed_into_integer_minor_units():
    connector = GoogleAdsConnector(today=date(2026, 8, 20), sleep_fn=lambda s: None)
    result = connector.extract(_account("1112223330"), date(2026, 8, 15), date(2026, 8, 15))
    row = next(m for m in result.metrics if m.platform_campaign_id == "6001")
    # 1,450,750,000 micros -> 145075 cents ($1450.75), integer division, no float drift
    assert row.spend_minor == 145075
