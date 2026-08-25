from datetime import date
from decimal import Decimal

import pytest

from adpipeline.currency import FxRateTable, FxRateUnavailable, convert_minor_units


@pytest.fixture
def fx_table():
    return FxRateTable(
        {
            (date(2026, 8, 15), "INR"): Decimal("0.01198"),
            (date(2026, 8, 15), "GBP"): Decimal("1.27"),
            (date(2026, 8, 15), "EUR"): Decimal("1.09"),
        }
    )


def test_same_currency_is_noop(fx_table):
    amount, rate = convert_minor_units(150000, "USD", "USD", date(2026, 8, 15), fx_table)
    assert amount == 150000
    assert rate == Decimal("1")


def test_inr_to_usd_conversion(fx_table):
    # 1450.75 INR -> minor units 145075
    amount, rate = convert_minor_units(145075, "INR", "USD", date(2026, 8, 15), fx_table)
    # 1450.75 * 0.01198 = 17.380585 -> 1738 minor units (rounded)
    assert amount == 1738
    assert rate == Decimal("0.01198")


def test_cross_rate_via_usd_pivot(fx_table):
    # GBP -> EUR should go via USD cross-rate, not a direct GBP/EUR rate.
    amount, rate = convert_minor_units(10000, "GBP", "EUR", date(2026, 8, 15), fx_table)
    expected_rate = Decimal("1.27") / Decimal("1.09")
    assert rate == expected_rate
    assert amount == int((Decimal("100") * expected_rate * 100).to_integral_value())


def test_missing_fx_rate_raises(fx_table):
    with pytest.raises(FxRateUnavailable):
        convert_minor_units(1000, "JPY", "USD", date(2026, 8, 15), fx_table)


def test_rounding_uses_banker_rounding_not_always_up(fx_table):
    # Construct a case that lands exactly on .5 cents to prove ROUND_HALF_EVEN
    # is in effect rather than naive float rounding (which could drift upward
    # systematically across millions of rows -- see DESIGN.md ยง4).
    table = FxRateTable({(date(2026, 8, 15), "XXX"): Decimal("1.005")})
    # 100 minor units (1.00 XXX) * 1.005 = 1.005 USD = 100.5 minor units -> rounds to 100 (even)
    amount, _ = convert_minor_units(100, "XXX", "USD", date(2026, 8, 15), table)
    assert amount == 100


def test_zero_spend_converts_to_zero(fx_table):
    amount, rate = convert_minor_units(0, "INR", "USD", date(2026, 8, 15), fx_table)
    assert amount == 0
