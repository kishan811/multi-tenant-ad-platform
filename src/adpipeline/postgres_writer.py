"""Idempotent Postgres writer.

Idempotency contract: re-running the same extraction for the same
(ad_account, campaign, reporting_day) must produce the same end state,
regardless of how many times it runs or whether a previous run crashed
mid-write. We get this via:

  1. A DB-level unique index on (ad_account_id, campaign_sk, reporting_day)
     (ux_fact_natural_key in migrations/001_init.sql).
  2. INSERT ... ON CONFLICT ... DO UPDATE keyed on that same tuple -- never
     INSERT-then-check or SELECT-then-decide (that has a race under
     concurrent workers; ON CONFLICT is a single atomic statement).
  3. Each extraction batch is wrapped in one transaction, so a crash
     mid-batch leaves the previous committed state untouched -- the next
     run simply reprocesses the same date range from scratch (safe because
     of #1/#2, not despite it).

dim_ad_account / dim_customer rows are assumed pre-seeded (they change
rarely and require human/admin action -- onboarding a customer). dim_campaign
rows are upserted here because new campaigns appear continuously and we
can't create them out-of-band.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from uuid import UUID


@dataclass(frozen=True)
class FactRow:
    customer_id: int
    ad_account_id: int
    platform_id: int
    platform_campaign_id: str
    campaign_name: str
    reporting_day: date
    spend_minor_source: int
    source_currency: str
    spend_minor_reporting: int
    reporting_currency: str
    fx_rate_used: Decimal
    fx_rate_date: date
    impressions: int
    clicks: int
    conversions: float
    attribution_window_days: int
    is_final: bool
    run_id: UUID


class PostgresWriter:
    def __init__(self, conn):
        self.conn = conn

    def upsert_campaign_daily(self, rows: list[FactRow]) -> int:
        if not rows:
            return 0
        written = 0
        # transaction(), not "with self.conn:" -- the latter closes the connection
        # on exit in psycopg3, which breaks a writer instance used across multiple
        # batches (e.g. one per customer in a single CLI run).
        with self.conn.transaction():
            with self.conn.cursor() as cur:
                for row in rows:
                    cur.execute(
                        """
                        INSERT INTO ads.dim_campaign (ad_account_id, platform_campaign_id, campaign_name)
                        VALUES (%(ad_account_id)s, %(platform_campaign_id)s, %(campaign_name)s)
                        ON CONFLICT (ad_account_id, platform_campaign_id)
                        DO UPDATE SET campaign_name = EXCLUDED.campaign_name, last_seen_at = now()
                        RETURNING campaign_sk
                        """,
                        {
                            "ad_account_id": row.ad_account_id,
                            "platform_campaign_id": row.platform_campaign_id,
                            "campaign_name": row.campaign_name,
                        },
                    )
                    (campaign_sk,) = cur.fetchone()

                    cur.execute(
                        """
                        INSERT INTO ads.fact_campaign_performance (
                            customer_id, ad_account_id, campaign_sk, platform_id, reporting_day,
                            spend_minor_source, source_currency, spend_minor_reporting, reporting_currency,
                            fx_rate_used, fx_rate_date, impressions, clicks, conversions,
                            attribution_window_days, is_final, last_extraction_run_id, updated_at
                        ) VALUES (
                            %(customer_id)s, %(ad_account_id)s, %(campaign_sk)s, %(platform_id)s, %(reporting_day)s,
                            %(spend_minor_source)s, %(source_currency)s, %(spend_minor_reporting)s, %(reporting_currency)s,
                            %(fx_rate_used)s, %(fx_rate_date)s, %(impressions)s, %(clicks)s, %(conversions)s,
                            %(attribution_window_days)s, %(is_final)s, %(run_id)s, now()
                        )
                        ON CONFLICT (ad_account_id, campaign_sk, reporting_day)
                        DO UPDATE SET
                            spend_minor_source = EXCLUDED.spend_minor_source,
                            source_currency = EXCLUDED.source_currency,
                            spend_minor_reporting = EXCLUDED.spend_minor_reporting,
                            reporting_currency = EXCLUDED.reporting_currency,
                            fx_rate_used = EXCLUDED.fx_rate_used,
                            fx_rate_date = EXCLUDED.fx_rate_date,
                            impressions = EXCLUDED.impressions,
                            clicks = EXCLUDED.clicks,
                            conversions = EXCLUDED.conversions,
                            attribution_window_days = EXCLUDED.attribution_window_days,
                            is_final = EXCLUDED.is_final,
                            last_extraction_run_id = EXCLUDED.last_extraction_run_id,
                            updated_at = now()
                        -- Only rewrite when something actually changed, so unrelated
                        -- concurrent readers don't see updated_at churn on no-op reruns.
                        WHERE ads.fact_campaign_performance.spend_minor_source IS DISTINCT FROM EXCLUDED.spend_minor_source
                           OR ads.fact_campaign_performance.conversions IS DISTINCT FROM EXCLUDED.conversions
                           OR ads.fact_campaign_performance.is_final IS DISTINCT FROM EXCLUDED.is_final
                        """,
                        {
                            "customer_id": row.customer_id,
                            "ad_account_id": row.ad_account_id,
                            "campaign_sk": campaign_sk,
                            "platform_id": row.platform_id,
                            "reporting_day": row.reporting_day,
                            "spend_minor_source": row.spend_minor_source,
                            "source_currency": row.source_currency,
                            "spend_minor_reporting": row.spend_minor_reporting,
                            "reporting_currency": row.reporting_currency,
                            "fx_rate_used": row.fx_rate_used,
                            "fx_rate_date": row.fx_rate_date,
                            "impressions": row.impressions,
                            "clicks": row.clicks,
                            "conversions": row.conversions,
                            "attribution_window_days": row.attribution_window_days,
                            "is_final": row.is_final,
                            "run_id": row.run_id,
                        },
                    )
                    written += 1
        return written
