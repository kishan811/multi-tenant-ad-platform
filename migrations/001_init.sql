-- =====================================================================
-- Multi-tenant ad platform analytics: initial schema
-- Postgres 14+. Uses native partitioning (RANGE by month) on facts.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS ads;

-- ---------------------------------------------------------------------
-- DIMENSIONS
-- ---------------------------------------------------------------------

CREATE TABLE ads.dim_customer (
    customer_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    external_key        TEXT NOT NULL UNIQUE,          -- your internal tenant slug
    display_name        TEXT NOT NULL,
    reporting_timezone   TEXT NOT NULL,                  -- IANA tz, e.g. 'America/New_York'
    reporting_currency   CHAR(3) NOT NULL,                -- ISO 4217, e.g. 'USD'
    refresh_cadence      TEXT NOT NULL DEFAULT 'daily',   -- 'daily' | 'hourly'
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- History of reporting_timezone / reporting_currency changes.
-- A change here is a business event that can trigger backfill (see DESIGN.md ยง3/ยง4).
CREATE TABLE ads.dim_customer_setting_history (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id         BIGINT NOT NULL REFERENCES ads.dim_customer(customer_id),
    attribute           TEXT NOT NULL CHECK (attribute IN ('reporting_timezone','reporting_currency')),
    old_value           TEXT,
    new_value           TEXT NOT NULL,
    effective_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    backfill_status      TEXT NOT NULL DEFAULT 'pending' CHECK (backfill_status IN ('pending','running','done','not_needed')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ads.dim_platform (
    platform_id         SMALLINT PRIMARY KEY,
    platform_code       TEXT NOT NULL UNIQUE  -- 'meta' | 'google_ads' | 'tiktok'
);
INSERT INTO ads.dim_platform (platform_id, platform_code) VALUES
    (1, 'meta'), (2, 'google_ads'), (3, 'tiktok')
ON CONFLICT DO NOTHING;

CREATE TABLE ads.dim_ad_account (
    ad_account_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id          BIGINT NOT NULL REFERENCES ads.dim_customer(customer_id),
    platform_id          SMALLINT NOT NULL REFERENCES ads.dim_platform(platform_id),
    platform_account_id  TEXT NOT NULL,          -- e.g. 'act_123456' for Meta
    account_timezone     TEXT NOT NULL,          -- IANA tz the PLATFORM reports the account in (often fixed, e.g. Meta accounts are UTC-ish per account tz; Google Ads accounts have a fixed tz)
    account_currency     CHAR(3) NOT NULL,       -- native currency the platform bills/report in for this account
    is_active            BOOLEAN NOT NULL DEFAULT TRUE,
    token_status         TEXT NOT NULL DEFAULT 'ok' CHECK (token_status IN ('ok','expiring_soon','expired','revoked')),
    last_successful_pull_at TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (platform_id, platform_account_id)
);
CREATE INDEX ix_ad_account_customer ON ads.dim_ad_account(customer_id);

CREATE TABLE ads.dim_campaign (
    campaign_sk          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ad_account_id        BIGINT NOT NULL REFERENCES ads.dim_ad_account(ad_account_id),
    platform_campaign_id TEXT NOT NULL,
    campaign_name        TEXT NOT NULL,
    objective            TEXT,
    status               TEXT,
    first_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ad_account_id, platform_campaign_id)
);

-- Daily FX rate table. Granularity = daily close, source currency -> USD (pivot),
-- so any pair converts via USD cross-rate. See DESIGN.md ยง4.
CREATE TABLE ads.dim_fx_rate (
    rate_date            DATE NOT NULL,
    currency_code        CHAR(3) NOT NULL,
    usd_per_unit         NUMERIC(18,8) NOT NULL,   -- 1 unit of currency_code = usd_per_unit USD
    source               TEXT NOT NULL DEFAULT 'openexchangerates',
    fetched_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (rate_date, currency_code)
);

-- ---------------------------------------------------------------------
-- STAGING: raw-ish landing zone, one row per (account, campaign, report_date, pulled_at)
-- Append-only. Never updated in place -> gives us a full audit trail for
-- attribution-window revisions (see DESIGN.md ยง5).
-- ---------------------------------------------------------------------

CREATE TABLE ads.stg_campaign_daily (
    id                    BIGINT GENERATED ALWAYS AS IDENTITY,
    ad_account_id         BIGINT NOT NULL REFERENCES ads.dim_ad_account(ad_account_id),
    platform_campaign_id  TEXT NOT NULL,
    campaign_name         TEXT,
    report_date           DATE NOT NULL,           -- date in the AD ACCOUNT's timezone, as returned by platform
    spend_minor            BIGINT NOT NULL,          -- integer minor units (cents) in account_currency
    impressions            BIGINT NOT NULL DEFAULT 0,
    clicks                 BIGINT NOT NULL DEFAULT 0,
    conversions             NUMERIC(18,4) NOT NULL DEFAULT 0,
    account_currency        CHAR(3) NOT NULL,
    extraction_run_id       UUID NOT NULL,
    pulled_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload              JSONB,                  -- full API response row, for debugging/replay
    PRIMARY KEY (id, report_date)  -- partitioned tables require the partition column in the PK
) PARTITION BY RANGE (report_date);

-- Rolling monthly staging partitions. In production a scheduled job creates next
-- month's partition ahead of time (not implemented here -- these two are seeded
-- manually so the take-home demo has somewhere to land Aug/Sep 2026 rows).
CREATE TABLE ads.stg_campaign_daily_2026_08 PARTITION OF ads.stg_campaign_daily
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE ads.stg_campaign_daily_2026_09 PARTITION OF ads.stg_campaign_daily
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');

CREATE INDEX ix_stg_lookup ON ads.stg_campaign_daily (ad_account_id, platform_campaign_id, report_date);

-- ---------------------------------------------------------------------
-- FACT: one row per (customer, ad_account, campaign, reporting_day).
-- reporting_day is the date bucket AFTER converting the account's
-- report_date into the CUSTOMER's reporting timezone (see DESIGN.md ยง3).
-- This is the idempotent upsert target: unique key below is the
-- conflict target for INSERT ... ON CONFLICT DO UPDATE.
-- ---------------------------------------------------------------------

CREATE TABLE ads.fact_campaign_performance (
    fact_id                BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id            BIGINT NOT NULL REFERENCES ads.dim_customer(customer_id),
    ad_account_id          BIGINT NOT NULL REFERENCES ads.dim_ad_account(ad_account_id),
    campaign_sk            BIGINT NOT NULL REFERENCES ads.dim_campaign(campaign_sk),
    platform_id            SMALLINT NOT NULL REFERENCES ads.dim_platform(platform_id),
    reporting_day           DATE NOT NULL,             -- customer-local calendar day
    spend_minor_source       BIGINT NOT NULL,           -- in account_currency minor units
    source_currency          CHAR(3) NOT NULL,
    spend_minor_reporting     BIGINT NOT NULL,           -- converted to customer reporting_currency, minor units
    reporting_currency        CHAR(3) NOT NULL,
    fx_rate_used              NUMERIC(18,8) NOT NULL,
    fx_rate_date               DATE NOT NULL,
    impressions                BIGINT NOT NULL DEFAULT 0,
    clicks                     BIGINT NOT NULL DEFAULT 0,
    conversions                 NUMERIC(18,4) NOT NULL DEFAULT 0,
    attribution_window_days      SMALLINT NOT NULL DEFAULT 7,
    is_final                    BOOLEAN NOT NULL DEFAULT FALSE,   -- true once past max attribution window
    last_extraction_run_id       UUID NOT NULL,
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (fact_id, reporting_day)
) PARTITION BY RANGE (reporting_day);

CREATE TABLE ads.fact_campaign_performance_2026_08 PARTITION OF ads.fact_campaign_performance
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE ads.fact_campaign_performance_2026_09 PARTITION OF ads.fact_campaign_performance
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');

-- The idempotency contract: re-running the same (account, campaign, day) extraction
-- must UPDATE this row, never duplicate it.
CREATE UNIQUE INDEX ux_fact_natural_key
    ON ads.fact_campaign_performance (ad_account_id, campaign_sk, reporting_day);

CREATE INDEX ix_fact_customer_day ON ads.fact_campaign_performance (customer_id, reporting_day);
CREATE INDEX ix_fact_not_final ON ads.fact_campaign_performance (reporting_day) WHERE NOT is_final;

-- ---------------------------------------------------------------------
-- OPERATIONAL: extraction run ledger (idempotency + observability)
-- ---------------------------------------------------------------------

CREATE TABLE ads.extraction_run (
    run_id             UUID PRIMARY KEY,
    ad_account_id      BIGINT NOT NULL REFERENCES ads.dim_ad_account(ad_account_id),
    date_range_start   DATE NOT NULL,
    date_range_end     DATE NOT NULL,
    status             TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running','succeeded','failed','partial')),
    rows_written        INT NOT NULL DEFAULT 0,
    error_message        TEXT,
    started_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at            TIMESTAMPTZ
);
CREATE INDEX ix_run_account_status ON ads.extraction_run (ad_account_id, status, started_at DESC);

-- ---------------------------------------------------------------------
-- RETENTION (see DESIGN.md ยง7)
-- stg_campaign_daily: keep 90 days of raw pulls, then drop partitions (cheap: DROP TABLE).
-- fact_campaign_performance: keep indefinitely (this is the product), but only
-- 13 months live in "hot" storage tier if cost becomes an issue -- see DESIGN.md ยง8.
-- ---------------------------------------------------------------------

-- ---------------------------------------------------------------------
-- SAMPLE QUERY: "yesterday's spend in USD for customer X"
-- Note "yesterday" is customer-local yesterday, resolved by their reporting_timezone.
-- ---------------------------------------------------------------------
-- WITH cust AS (
--     SELECT customer_id, reporting_timezone
--     FROM ads.dim_customer
--     WHERE external_key = 'acme-d2c'
-- )
-- SELECT
--     f.customer_id,
--     f.reporting_day,
--     SUM(f.spend_minor_reporting) / 100.0 AS spend_usd,
--     BOOL_AND(f.is_final) AS all_rows_final
-- FROM ads.fact_campaign_performance f
-- JOIN cust c USING (customer_id)
-- WHERE f.reporting_day = (now() AT TIME ZONE c.reporting_timezone)::date - 1
--   AND f.reporting_currency = 'USD'
-- GROUP BY f.customer_id, f.reporting_day;
