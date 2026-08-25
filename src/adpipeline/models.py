from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class RawCampaignDayMetric:
    """Normalized shape every connector must produce, regardless of platform.
    This is the ~70%-shared contract: connectors differ only in how they get here.
    """
    platform: str
    platform_account_id: str
    platform_campaign_id: str
    campaign_name: str
    report_date: date              # bucketed in the account's own timezone
    account_timezone: str
    account_currency: str
    spend_minor: int                # integer minor units, never float
    impressions: int
    clicks: int
    conversions: float
    is_reporting_lag_final: bool    # False if platform flags this row as still within its attribution window
