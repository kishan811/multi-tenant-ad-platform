"""Google Ads connector.

Platform-specific quirks handled here (everything else -- pagination
draining, retry/backoff, token-bucket rate limiting -- is inherited from
BaseConnector, see base.py and mock_google_ads_api.py's module docstring
for the full quirk-by-quirk comparison against Meta):

  - `metrics.costMicros` is an integer count of micros (1,000,000 micros = 1
    currency unit), returned as a STRING over the REST transport -- parsed
    directly to integer minor units via `// 10_000` (10,000 micros = 1 cent),
    not via Decimal/string parsing like Meta's `spend` field.
  - `metrics.conversions` is a flat float field, no `actions[]` array to search
    (contrast with Meta's `_extract_conversions`).
  - No platform-supplied finality flag: this connector always reports
    `is_reporting_lag_final=True`, which sounds backwards until you read how
    the pipeline combines it (`pipeline.py::_to_fact_row`): finality is
    `platform_flag AND days_old >= attribution_window_days`. Meta's flag can
    genuinely veto an old-enough row (rare, but the platform knows something
    we don't); Google Ads has no such signal to contribute, so it reports
    "no veto" (True) and defers entirely to the pipeline's own age-based
    rule -- deliberately weaker than Meta's explicit confirmation, and worth
    calling out in review (see DESIGN.md ยง5.1).
  - `RESOURCE_EXHAUSTED` -> RateLimitedError with `retry_after_seconds=None`
    (the platform gives no hint, so the shared backoff falls back to pure
    exponential-with-jitter); `UNAUTHENTICATED` / `PERMISSION_DENIED` -> AuthError.
"""
from __future__ import annotations

from datetime import date, datetime

from adpipeline.config import AdAccountConfig
from adpipeline.base_connector import AuthError, BaseConnector, RateLimitedError, TransientApiError
from adpipeline.mock_google_ads_api import MockGoogleAdsClient, MockGoogleAdsError
from adpipeline.models import RawCampaignDayMetric

MICROS_PER_CENT = 10_000  # 1,000,000 micros per currency unit / 100 cents per unit


class GoogleAdsConnector(BaseConnector):
    platform = "google_ads"

    def __init__(self, client: MockGoogleAdsClient | None = None, today: date | None = None, **kwargs):
        super().__init__(**kwargs)
        self.client = client or MockGoogleAdsClient()
        # Injectable "current date" so reporting-lag maturity is deterministic in tests.
        self._today = today or datetime.utcnow().date()

    def fetch_page(self, account: AdAccountConfig, start: date, end: date, page_token: str | None):
        try:
            return self.client.search(
                account.platform_account_id, start, end, page_token, today=self._today
            )
        except MockGoogleAdsError as exc:
            if exc.status == "RESOURCE_EXHAUSTED":
                raise RateLimitedError(retry_after_seconds=None) from exc
            if exc.status in ("UNAUTHENTICATED", "PERMISSION_DENIED"):
                raise AuthError(exc.message) from exc
            raise TransientApiError(str(exc)) from exc

    def parse_rows(self, raw_page) -> tuple[list[RawCampaignDayMetric], str | None]:
        rows = []
        for r in raw_page.get("results", []):
            metrics = r["metrics"]
            spend_minor = int(metrics["costMicros"]) // MICROS_PER_CENT
            rows.append(
                RawCampaignDayMetric(
                    platform=self.platform,
                    platform_account_id=r["campaign"]["resourceName"].split("/")[1],
                    platform_campaign_id=r["campaign"]["id"],
                    campaign_name=r["campaign"]["name"],
                    report_date=date.fromisoformat(r["segments"]["date"]),
                    account_timezone="",  # filled in by pipeline from account config
                    account_currency="",  # filled in by pipeline from account config
                    spend_minor=spend_minor,
                    impressions=int(metrics["impressions"]),
                    clicks=int(metrics["clicks"]),
                    conversions=float(metrics["conversions"]),
                    is_reporting_lag_final=True,  # no platform signal to veto on; pipeline's age check decides
                )
            )
        next_token = raw_page.get("nextPageToken")
        return rows, next_token
