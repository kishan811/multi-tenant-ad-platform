"""Timezone bucketing: account-local report_date -> customer-local reporting_day.

Trace: a click at 23:00 IST (Asia/Kolkata, UTC+5:30, no DST) is what the ad
platform's API attributes to the AD ACCOUNT's calendar day, because Meta/Google/
TikTok all bucket a click into "the day it happened in the account's own
timezone" before we ever see it -- we cannot re-derive it from raw event
timestamps, only from the (account_timezone, report_date) pair the API hands
back. So the pipeline's job is a *day-bucket-to-day-bucket* conversion, not a
timestamp conversion: we don't have the click's exact instant, only the
account-local calendar date it landed on.

To convert that into the customer's reporting day we assume the click landed
at local NOON on report_date in the account timezone (a deliberate
approximation -- see DESIGN.md ยง3 for why exact-instant conversion is not
possible with daily-grain platform APIs, and what we'd do differently with
hourly-grain data).
"""
from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo


def account_day_to_reporting_day(
    report_date: date,
    account_timezone: str,
    reporting_timezone: str,
) -> date:
    """Map a platform report_date (bucketed in account_timezone) to the
    customer's reporting-day bucket (in reporting_timezone).

    Uses local-noon-of-report_date as the representative instant. This is
    exact when account_timezone == reporting_timezone (the common case --
    most customers report in the same tz as their primary ad account), and
    is a bounded, documented approximation otherwise (see DESIGN.md ยง3).
    """
    if account_timezone == reporting_timezone:
        return report_date

    account_tz = ZoneInfo(account_timezone)
    reporting_tz = ZoneInfo(reporting_timezone)

    representative_instant = datetime.combine(report_date, time(12, 0), tzinfo=account_tz)
    return representative_instant.astimezone(reporting_tz).date()


def is_dst_transition_day(day: date, tz_name: str) -> bool:
    """True if the UTC offset changes at any point during `day` in `tz_name`.

    Used to flag reporting days that span a US DST transition (spring-forward
    23h day / fall-back 25h day) so operators know why a day's hourly-grain
    numbers might look off if hourly data is ever added for this tz.
    """
    tz = ZoneInfo(tz_name)
    start_offset = datetime.combine(day, time(0, 0), tzinfo=tz).utcoffset()
    end_offset = datetime.combine(day, time(23, 59), tzinfo=tz).utcoffset()
    return start_offset != end_offset
