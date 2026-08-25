"""Currency conversion: account_currency (source) -> reporting_currency.

Rates are daily-close, one row per (date, currency) priced against USD (the
pivot currency), stored in ads.dim_fx_rate. Any pair converts via a USD
cross-rate: amount_in_target = amount_in_source / usd_per_unit[source] * usd_per_unit[target].

All monetary amounts are handled in *minor units* (cents) as integers end to
end, and we only round once, at the very last step of conversion, using
ROUND_HALF_EVEN (banker's rounding) to avoid systematic upward drift across
millions of daily rows -- see DESIGN.md ยง4.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_EVEN, Decimal


class FxRateUnavailable(Exception):
    pass


@dataclass(frozen=True)
class FxRate:
    rate_date: date
    currency_code: str
    usd_per_unit: Decimal


class FxRateTable:
    """In-memory lookup used by the pipeline; backed by ads.dim_fx_rate in prod."""

    def __init__(self, rates: dict[tuple[date, str], Decimal]):
        self._rates = rates

    def usd_per_unit(self, rate_date: date, currency: str) -> Decimal:
        if currency == "USD":
            return Decimal("1")
        try:
            return self._rates[(rate_date, currency)]
        except KeyError as exc:
            raise FxRateUnavailable(f"No FX rate for {currency} on {rate_date}") from exc


def convert_minor_units(
    amount_minor: int,
    source_currency: str,
    target_currency: str,
    rate_date: date,
    fx_table: FxRateTable,
) -> tuple[int, Decimal]:
    """Returns (converted_amount_minor, fx_rate_used) where fx_rate_used is the
    source->target multiplier actually applied (stored for audit/backfill math).
    """
    if source_currency == target_currency:
        return amount_minor, Decimal("1")

    source_usd = fx_table.usd_per_unit(rate_date, source_currency)
    target_usd = fx_table.usd_per_unit(rate_date, target_currency)
    rate = source_usd / target_usd  # 1 unit source -> this many units target

    amount_source = Decimal(amount_minor) / Decimal(100)
    amount_target = amount_source * rate
    amount_target_minor = int(
        (amount_target * 100).to_integral_value(rounding=ROUND_HALF_EVEN)
    )
    return amount_target_minor, rate
