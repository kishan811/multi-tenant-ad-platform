"""A fixture-driven fake of the Google Ads API (`GoogleAdsService.Search` over
a GAQL report query against the `campaign` resource, segmented by `segments.date`).

Simulates the specific quirks that make Google Ads a different integration
problem than Meta, even though the shared retry/pagination/normalization
plumbing (base.py) is identical:

  - **Pagination**: `next_page_token` in the response body (not a cursor
    object like Meta's `paging.cursors.after`), and Google Ads pages are
    request-shaped (you resend the same GAQL query with `page_token` set)
    rather than URL-shaped.
  - **Cost is in micros**, an integer (`metrics.cost_micros`), not a decimal
    string like Meta's `spend`. 1,000,000 micros = 1 unit of the account's
    currency. This is actually *less* error-prone than Meta's string-parsing
    (no locale/decimal-point ambiguity) but the connector still has to know
    the micros convention, or every cost figure is off by 6 orders of magnitude.
  - **No explicit "is this row final" flag.** Meta tells you `date_stop_is_final`;
    Google Ads just silently revises `metrics.conversions` on re-query within
    the conversion lag window and gives you nothing to distinguish a fresh
    row from a stale one -- finality has to be inferred purely from
    `today - segments.date >= conversion_lag_window`, which is a strictly
    weaker signal than Meta's explicit flag (see DESIGN.md ยง1.3 / ยง5.1).
  - **Rate limiting** surfaces as a `RESOURCE_EXHAUSTED` gRPC-style status
    (mapped to HTTP 429 on the REST transport), with *no* Retry-After hint --
    unlike Meta's 613 error, which at least tells you how long to wait. The
    connector has to fall back to blind exponential backoff for this platform.
  - **Auth failures** are `UNAUTHENTICATED` (expired OAuth refresh token) or
    `PERMISSION_DENIED` (revoked access / unlinked account, or a suspended
    developer token) -- different error shape than Meta's single `code: 190`.

No network calls. Deterministic given (customer_id, date_range, call_count).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


class MockGoogleAdsError(Exception):
    def __init__(self, status: str, message: str):
        self.status = status  # 'RESOURCE_EXHAUSTED' | 'UNAUTHENTICATED' | 'PERMISSION_DENIED'
        self.message = message
        super().__init__(f"[{status}] {message}")


@dataclass
class _FixtureCampaign:
    campaign_id: str
    campaign_name: str
    base_cost_micros: int
    base_impressions: int
    base_clicks: int
    base_conversions: float


_FIXTURE_CAMPAIGNS = {
    "1112223330": [  # Acme D2C, India account
        _FixtureCampaign("6001", "Acme - Search - Brand - IN", 1_450_750_000, 82000, 1890, 42.0),
        _FixtureCampaign("6002", "Acme - PMax - IN", 630_200_000, 21000, 980, 61.0),
    ],
    "2223334440": [  # Glow Cosmetics, US account 1
        _FixtureCampaign("6101", "Glow - Search - Brand - US", 980_000_000, 45000, 1200, 28.0),
    ],
    "2223334441": [  # Glow Cosmetics, US account 2
        _FixtureCampaign("6201", "Glow - PMax - US", 210_550_000, 9000, 410, 19.0),
    ],
    "9998887770": [],  # revoked
    "9998887771": [],  # expired oauth token
}

PAGE_SIZE = 1  # force multi-page fixtures even with few campaigns, to exercise pagination logic

# Google Ads conversion lag can extend up to 90 days for some conversion actions,
# but the pipeline's default (see pipeline.py) mirrors the platform's own UI default
# reporting window of 30 days -- long enough to catch the vast majority of lagged
# conversions without holding every account's "not yet final" window open forever.
GOOGLE_ADS_DEFAULT_CONVERSION_LAG_DAYS = 30


class MockGoogleAdsClient:
    """Simulates per-customer-account call counters so repeated calls behave
    consistently within one test/run, matching real rate-limit / lag behavior.
    """

    def __init__(self):
        self._call_count: dict[str, int] = {}

    def search(
        self,
        customer_id: str,
        start: date,
        end: date,
        page_token: str | None,
        today: date,
    ) -> dict:
        if customer_id == "9998887770":
            raise MockGoogleAdsError("PERMISSION_DENIED", "User doesn't have permission to access customer. Likely cause: account access was revoked.")
        if customer_id == "9998887771":
            raise MockGoogleAdsError("UNAUTHENTICATED", "Request had invalid authentication credentials. Expected OAuth 2 access token, refresh token has expired.")

        self._call_count[customer_id] = self._call_count.get(customer_id, 0) + 1
        call_n = self._call_count[customer_id]

        # Every 3rd call for this account gets rate-limited, to exercise retry/backoff --
        # deliberately with no retry-after hint, unlike the Meta mock.
        if call_n % 3 == 0:
            raise MockGoogleAdsError("RESOURCE_EXHAUSTED", "Too many requests. Retry after a backoff period.")

        campaigns = _FIXTURE_CAMPAIGNS.get(customer_id, [])
        offset = int(page_token) if page_token else 0
        page_campaigns = campaigns[offset : offset + PAGE_SIZE]

        results = []
        for camp in page_campaigns:
            day = start
            while day <= end:
                days_old = (today - day).days
                maturity = min(days_old / GOOGLE_ADS_DEFAULT_CONVERSION_LAG_DAYS, 1.0) if days_old >= 0 else 0.0
                results.append(
                    {
                        "campaign": {
                            "id": camp.campaign_id,
                            "name": camp.campaign_name,
                            "resourceName": f"customers/{customer_id}/campaigns/{camp.campaign_id}",
                        },
                        "segments": {"date": day.isoformat()},
                        "metrics": {
                            "costMicros": str(camp.base_cost_micros),  # REST transport returns int64 as string
                            "impressions": str(camp.base_impressions),
                            "clicks": str(camp.base_clicks),
                            "conversions": camp.base_conversions * maturity,
                        },
                    }
                )
                day += timedelta(days=1)

        next_offset = offset + PAGE_SIZE
        next_page_token = str(next_offset) if next_offset < len(campaigns) else None

        response = {"results": results}
        if next_page_token:
            response["nextPageToken"] = next_page_token
        return response
