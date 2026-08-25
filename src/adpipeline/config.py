"""Customer / ad-account configuration loading.

In production this would read from ads.dim_customer / ads.dim_ad_account
(Postgres is the source of truth). For the take-home, a YAML file stands
in for that table so the pipeline is runnable without a pre-seeded DB.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml


@dataclass(frozen=True)
class AdAccountConfig:
    platform: str
    platform_account_id: str
    account_timezone: str
    account_currency: str


@dataclass(frozen=True)
class CustomerConfig:
    external_key: str
    display_name: str
    reporting_timezone: str
    reporting_currency: str
    refresh_cadence: str
    ad_accounts: tuple[AdAccountConfig, ...]

    def __post_init__(self) -> None:
        # Fail fast on bad tz/currency data rather than silently mis-bucketing spend.
        _validate_timezone(self.reporting_timezone)
        _validate_currency(self.reporting_currency)
        for acct in self.ad_accounts:
            _validate_timezone(acct.account_timezone)
            _validate_currency(acct.account_currency)


def _validate_timezone(tz_name: str) -> None:
    try:
        ZoneInfo(tz_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown IANA timezone: {tz_name!r}") from exc


def _validate_currency(code: str) -> None:
    if not (len(code) == 3 and code.isalpha() and code.isupper()):
        raise ValueError(f"Currency code must be a 3-letter ISO 4217 code, got {code!r}")


def load_customers(path: str | Path) -> list[CustomerConfig]:
    raw = yaml.safe_load(Path(path).read_text())
    customers = []
    for entry in raw["customers"]:
        accounts = tuple(
            AdAccountConfig(
                platform=a["platform"],
                platform_account_id=a["platform_account_id"],
                account_timezone=a["account_timezone"],
                account_currency=a["account_currency"],
            )
            for a in entry["ad_accounts"]
        )
        customers.append(
            CustomerConfig(
                external_key=entry["external_key"],
                display_name=entry["display_name"],
                reporting_timezone=entry["reporting_timezone"],
                reporting_currency=entry["reporting_currency"],
                refresh_cadence=entry.get("refresh_cadence", "daily"),
                ad_accounts=accounts,
            )
        )
    return customers
