# Multi-Tenant Ad Platform Pipeline — Design Document

**Author:** Kishan · **Scope:** Meta, Google Ads, TikTok → Postgres, for a multi-tenant D2C analytics platform
**Scale target:** ~200 customers today → ~1,000 in 18 months, 1–5 ad accounts per platform per customer, daily baseline / hourly for some.

---

## 1. Architecture

### 1.1 Orchestration

I'd go with **Airflow**. It's the orchestrator most data platform teams already have running, so whoever ends up on-call already understands the operational surface — retries, SLAs, backfill via `dagrun` re-runs, the per-task-instance view in the UI. I did consider Dagster's asset-centric model, since a lot of this pipeline is naturally partition-shaped, but nothing here actually depends on Dagster to work well: the idempotent-upsert contract (§5) already does the job that Dagster's software-defined assets would otherwise do — telling you whether a given partition materialized correctly. Airflow's job is scheduling and retries, not correctness. Correctness lives in the database constraint. That's on purpose: it means the pipeline behaves the same whether you kick it off from Airflow, cron, or a laptop shell — which is exactly how this repo's own `cli.py` runs it.

- **DAG shape:** one DAG per platform (`meta_daily`, `google_ads_daily`, `tiktok_daily`), each with a single **dynamically task-mapped** extraction task — `extract.expand(ad_account=get_active_accounts_for_platform())`. The DAG ends up with exactly as many task instances as there are active accounts for that platform, discovered at run time from `dim_ad_account` rather than hardcoded anywhere. This is the Airflow-native way to get "one task per account" without a DAG-per-account explosion — 7,500 accounts at 1,000 customers would make for an unmanageable DAG folder otherwise.
- **Scheduling:** the daily DAG runs once a day, and each mapped task pulls "yesterday plus a trailing N-day reconciliation window" (§5) for its account. A separate hourly DAG, scheduled `@hourly`, only maps over accounts flagged `refresh_cadence = hourly` and re-pulls just today's partial day.
- **Backfill:** Airflow's own backfill (`airflow dags backfill -s <start> -e <end>`) re-runs the DAG over a historical date range, and that's fine for the routine attribution-window reconciliation case. But the two event-driven backfills elsewhere in this doc — a timezone change (§3.4) or a currency change (§4.4) — I would *not* try to force into Airflow's date-based backfill. Those are triggered by one specific customer changing a setting, not a calendar range that applies to everyone. A small operator-triggered DAG (`customer_backfill`, parameterized with `customer_id` and a date range, fired via Airflow's REST API) handles that case directly — same `extract → normalize → convert → upsert` code path, just invoked with different parameters rather than a separate pipeline.
- **Idempotency is what makes Airflow retries safe without extra work.** A task Airflow retries after a timeout, or a backfill that overlaps a day already materialized, both resolve to the same `INSERT ... ON CONFLICT DO UPDATE` (§5). I don't need any Airflow-side dedup logic on top of what the database already guarantees.
- [`dags/google_ads_daily.py`](../dags/google_ads_daily.py) makes this concrete rather than only descriptive — real TaskFlow/dynamic-task-mapping syntax, calling the exact same `run_customer_extraction()` that `cli.py` uses. It's illustrative, not a DAG I've run against a live Airflow scheduler (that needs real Airflow Connections and Pools, out of scope here) — see the README's assumptions section for that caveat spelled out.

### 1.2 Extraction workers

Each Airflow task instance is one `(platform, ad_account)` extraction, mapped dynamically as above, and I'd run it via `KubernetesPodOperator` (or `ECSOperator` on AWS) rather than a plain `PythonOperator` sitting in the scheduler's own worker pool — that way one account's extraction is resource-isolated and can't starve other tasks. It's also the natural failure-isolation boundary: Meta, Google Ads, and TikTok all rate-limit per ad account (§6), so a task's blast radius on a 429/613 is exactly one account and never bleeds into a customer's other accounts. At 1,000 customers, roughly 2 accounts each, across 3 platforms, that's a ceiling around 6,000 logical extraction tasks a day — comfortably inside Airflow's parallelism settings once nudged up a bit (20–50 concurrent task slots is plenty), since each task is short and I/O-bound.

Each task does exactly:
1. fetch config (customer + account row) from Postgres,
2. run the connector's `extract()`,
3. normalize → timezone-bucket → currency-convert,
4. idempotent upsert (§5),
5. write an `extraction_run` row (status, rows written, error) for observability (§6). I keep this in addition to Airflow's own task-instance status, on purpose — `extraction_run` is queryable by account or customer for on-call (§6.3) without touching the Airflow UI or metadata DB, and it outlives whatever log retention Airflow itself is configured with.

### 1.3 Why three connectors share ~70% of logic

Implemented in [`base_connector.py`](../src/adpipeline/base_connector.py). The shared 70% is basically everything that isn't about a specific platform's wire format:

| Shared (base class) | Platform-specific (subclass) |
|---|---|
| Pagination draining loop | Cursor shape: Meta uses `paging.cursors.after`; Google Ads uses a GAQL `LIMIT`/page-token via `search_stream`; TikTok uses `page_info.page` + total-page count |
| Retry/backoff with jitter | What counts as "rate limited": Meta returns HTTP 613-style app-level errors with an X-Business-Use-Case-Usage header; Google Ads returns gRPC `RESOURCE_EXHAUSTED` (HTTP 429 equivalent); TikTok returns HTTP 200 with a `code` field in the body (`40100`) — each connector maps its own signal into the shared `RateLimitedError` |
| Token-bucket rate limiting per account | Bucket capacity/refill differs per platform (Meta ad-account-level compute-time budget vs. Google Ads' operations-per-day quota vs. TikTok's requests-per-minute) |
| Auth-error classification (non-retryable) | Meta: `error.code == 190`; Google Ads: `UNAUTHENTICATED` / `PERMISSION_DENIED`; TikTok: `code == 40105` |
| Converting a raw row → `RawCampaignDayMetric` (the shared normalized contract) | Field mapping: Meta buries conversions in an `actions[]` array keyed by `action_type`; Google Ads has a flat `metrics.conversions` field; TikTok has `conversion` directly but splits "attributed" vs. "click-through" conversions across two fields |
| Reporting-lag / attribution-window flag surfacing | Meta: `date_stop_is_final`-style flag derived from window age; Google Ads: rows within the lookback window are just silently revised on re-pull, no flag at all, so we infer finality purely from row age; TikTok: an explicit `is_attribution_window_mature` boolean |

The piece that actually makes this work is the `RawCampaignDayMetric` contract ([`models.py`](../src/adpipeline/models.py)) — every connector, no matter the platform, has to produce a stream of these, and the pipeline (`pipeline.py`) never sees anything platform-specific past that boundary. It's the standard "adapter normalizes to a canonical event, core pipeline only knows the canonical event" pattern. Nothing clever about it, but it's what keeps the next connector cheap to add instead of a rewrite.

### 1.4 High-level data flow

```
 platform API (Meta/Google/TikTok)
        │  fetch_page() [connector-specific]
        ▼
 RawCampaignDayMetric  (account-tz report_date, account-currency spend, account-native fields)
        │  timezone_utils.account_day_to_reporting_day()
        │  currency.convert_minor_units()
        ▼
 FactRow  (customer-tz reporting_day, reporting-currency spend)
        │  PostgresWriter.upsert_campaign_daily()  [INSERT ... ON CONFLICT DO UPDATE]
        ▼
 ads.fact_campaign_performance
```

---

## 2. Postgres Schema

Full DDL: [`migrations/001_init.sql`](../migrations/001_init.sql). Here's the reasoning behind the main tables:

- **`ads.dim_customer`** — one row per tenant: `reporting_timezone` (an IANA name, not a fixed UTC offset — §3 gets into why that matters), `reporting_currency`, `refresh_cadence`.
- **`ads.dim_customer_setting_history`** — an append-only log of `reporting_timezone`/`reporting_currency` changes, each carrying a `backfill_status`. This exists specifically so a tz or currency change becomes a tracked event that drives a backfill job, rather than a silent config edit that leaves old rows computed under the previous setting (§3, §4).
- **`ads.dim_ad_account`** — one row per platform ad account, holding that account's own timezone and currency, which almost never match the customer's reporting tz/currency, plus `token_status` for auth health (§6).
- **`ads.dim_campaign`** — surrogate-keyed and upserted continuously, since campaigns appear and get renamed constantly. This is a type-1 slowly-changing dimension — we just track `last_seen_at`; full SCD2 history on campaign name isn't worth it here.
- **`ads.dim_fx_rate`** — daily-close rate per currency, priced against USD as the pivot (§4).
- **`ads.stg_campaign_daily`** — append-only, partitioned by month on `report_date`. Every extraction run lands new rows here, including the raw `raw_payload JSONB` of the API response. This is the audit trail: if a customer disputes a number, we can replay exactly what the platform returned on every pull, revisions included. Retention is 90 days (§7) — it's a debugging/replay tool, not the analytical surface.
- **`ads.fact_campaign_performance`** — the actual query surface. Partitioned by month on `reporting_day`, not `report_date` — the fact table lives in the customer's calendar, staging lives in the platform's. Grain is one row per `(ad_account_id, campaign_sk, reporting_day)`, enforced with a unique index that also doubles as the idempotent-upsert conflict target (§5).
- **`ads.extraction_run`** — a ledger of every extraction attempt (status, rows written, error). This backs both crash-safe idempotency (§5) and on-call debugging (§6).

### 2.1 Partitioning & indexing

- **Monthly range partitions** on both `stg_campaign_daily` and `fact_campaign_performance`. At 1,000 customers, ~2.5 accounts each, ~5 campaigns per account, 365 days a year, the fact table lands around **~4.5M rows/year** — not really big enough to *require* partitioning for query speed. But partitioning still buys two things worth having at this scale: staging retention becomes `DROP PARTITION` instead of a slow `DELETE`, and partition pruning keeps "last 30 days" dashboard queries fast no matter how many years of history pile up.
- **`ux_fact_natural_key`** — the unique index on `(ad_account_id, campaign_sk, reporting_day)` that is the idempotency key.
- **`ix_fact_customer_day`** on `(customer_id, reporting_day)` — this is the main dashboard access pattern, "customer X, last 30 days."
- **A partial index**, `ix_fact_not_final WHERE NOT is_final` — small and hot, covering exactly the rows the reconciliation job needs to re-check (§5), so that job isn't scanning the whole fact table every time it runs.

### 2.2 Retention

- `stg_campaign_daily`: 90 days, dropped by partition. Long enough to debug "what did the platform actually say two months ago," short enough to keep raw JSONB volume from getting out of hand.
- `fact_campaign_performance`: kept indefinitely — it's the actual product. If storage cost ever becomes a real concern, the lever is moving old partitions (older than ~13 months, say) to a cheaper storage tier (§8), not deleting anything.
- `extraction_run`: 180 days, enough for on-call and postmortem lookback. Older runs would roll up into a monthly summary table if we ever needed long-term SLA reporting.

### 2.3 Sample query: "yesterday's spend in USD for customer X"

```sql
WITH cust AS (
    SELECT customer_id, reporting_timezone
    FROM ads.dim_customer
    WHERE external_key = 'acme-d2c'
)
SELECT
    f.reporting_day,
    SUM(f.spend_minor_reporting) / 100.0 AS spend_usd,
    BOOL_AND(f.is_final)               AS all_rows_final
FROM ads.fact_campaign_performance f
JOIN cust c USING (customer_id)
WHERE f.reporting_day = (now() AT TIME ZONE c.reporting_timezone)::date - 1
  AND f.reporting_currency = 'USD'
GROUP BY f.reporting_day;
```

Notice the `all_rows_final` flag in the output — I surfaced that on purpose so a dashboard can say "yesterday: $12,430 (still finalizing)" instead of presenting attribution-incomplete numbers as though they're settled (§5).

---

## 3. Timezone Handling

### 3.1 Tracing a single click at 11pm IST

Here's the concrete case: someone in Mumbai clicks an ad at **23:00 IST on Aug 15**, on an Acme D2C ad account whose account timezone (set in the Meta Business Manager account) is `Asia/Kolkata` — which happens to be IST too — while the customer's actual reporting timezone is `America/New_York`, since Acme is a US-based brand running an India-targeted campaign.

1. **At the platform.** Meta attributes the click to `Asia/Kolkata`'s calendar day the moment it happens — bucketed into `report_date = 2026-08-15` inside Meta's own systems, before we ever call the API. All three platforms work this way: the platform does the first timezone bucketing, not us, using whatever timezone is configured on the ad account. We never see the click's raw UTC timestamp, only the day bucket it already landed in.
2. **At extraction.** Our Meta connector calls the `insights` endpoint for `date_start=2026-08-15`, and gets back aggregated spend/impressions/clicks/conversions for that account-local day — `RawCampaignDayMetric(report_date=2026-08-15, account_timezone="Asia/Kolkata", ...)`.
3. **At normalization.** `timezone_utils.account_day_to_reporting_day()` converts that account-local day bucket into the customer's reporting day. Since all we have is a day, not an instant, I anchor at local noon of `report_date` in the account timezone and convert that representative instant into the reporting timezone, then take its date. Working through the numbers: noon IST on Aug 15 is 06:30 UTC, which is 02:30 EDT — still Aug 15 in America/New_York. (A click near the account's local midnight is exactly where this approximation can land on the wrong day — more on that in 3.2.)
4. **At the fact table.** The row lands as `reporting_day = 2026-08-15` in `fact_campaign_performance`, with the account's `INR` already converted to the customer's `USD` (§4).

### 3.2 Where the approximation breaks, and why I accept it

Platform daily-report APIs hand us date buckets, not instants, so any account-day-to-customer-day mapping is inherently lossy whenever `account_timezone != reporting_timezone` and the offset doesn't happen to preserve day boundaries. A click at 23:55 IST really is in `report_date=Aug 15`, but the true click volume near midnight could fall on either calendar day once you look at it in the customer's timezone — there's no way to recover that sub-day distribution from a daily aggregate, full stop.

Given more time, I'd pull **hourly-grain reports** for customers who need sub-day precision across timezones — mostly the `refresh_cadence = hourly` cohort — since both Meta and Google Ads support it (TikTok's hourly breakdown is more limited), and bucket each hour independently into the customer's timezone. That shrinks the possible error from "up to a day" down to "up to an hour," which is close enough for essentially any practical purpose. It's also why the schema's grain is `(ad_account_id, campaign_sk, reporting_day)` rather than something instant-based — it can absorb an hourly-grain future without a schema change, just a finer-grained input.

For what it's worth, I'd expect `account_timezone == reporting_timezone` to hold for the large majority of D2C accounts in practice (I'd confirm this empirically rather than assume it, but an account is usually configured in the timezone of the business running it) — in which case the conversion is an exact no-op. `account_day_to_reporting_day` short-circuits on that case directly (see [`timezone_utils.py`](../src/adpipeline/timezone_utils.py)).

### 3.3 US DST

DST matters in two places:

1. **Day-length arithmetic.** `America/New_York` has a 23-hour day on spring-forward (second Sunday in March) and a 25-hour day on fall-back (first Sunday in November). Since we bucket by date, not elapsed hours, this doesn't corrupt spend attribution — Aug 15 stays Aug 15 regardless of DST. It would only bite us if we ever computed "hours since midnight" directly, which we don't. `is_dst_transition_day()` exists to flag these days for whoever eventually adds hourly-grain reporting, so they don't get burned by a naive `hour = (timestamp - midnight) / 3600` on a 23- or 25-hour day.
2. **`ZoneInfo` correctness.** We use IANA tz names (`America/New_York`), never fixed UTC offsets (`UTC-5`), specifically so `zoneinfo` resolves the right offset for the actual date being converted. It's a one-line decision — `account.account_timezone: str`, not `utc_offset_minutes: int` — that eliminates an entire category of DST bugs for free. Storing a fixed offset is one of the most common mistakes I've seen in analytics pipelines: it "works" in testing (usually run in a non-DST month) and then silently misattributes a day's data every March and November in production.

### 3.4 Reporting timezone changes trigger backfill

Say a customer changes `reporting_timezone` — expanding from US-only to EU and moving HQ reporting from `America/New_York` to `Europe/London`. Every historical `reporting_day` bucket computed under the old timezone is now wrong for any account whose `account_timezone` doesn't match the new reporting timezone. Here's the flow:

1. The config-change API writes a row to `ads.dim_customer_setting_history` (`attribute='reporting_timezone', old_value=..., new_value=..., backfill_status='pending'`) *before* updating `dim_customer.reporting_timezone` itself. That history row, not a before/after config diff, is the source of truth for "a backfill is owed."
2. A backfill job picks up `backfill_status='pending'` rows and re-materializes every partition for that customer's accounts, back to the start of the retention window, re-running the same `extract → normalize → upsert` pipeline with the new timezone. Because the upsert is idempotent and keyed on `(ad_account_id, campaign_sk, reporting_day)`, a shifted `reporting_day` doesn't collide with the old rows — it just produces a new key, leaving the old rows orphaned under the previous bucketing. **The backfill job has to explicitly delete or mark stale the old-bucketed rows for the affected range** — just inserting the new ones isn't enough, or the customer ends up seeing double-counted spend. This is the one place where idempotency-by-upsert alone doesn't cover it; it needs an explicit "supersede the old range" step.
3. `backfill_status` moves from `running` to `done`, and that transition is what on-call/support tooling would surface to the customer — "historical data updated to your new reporting timezone as of Aug 20."
4. The backfill only goes as far back as data retention, not all of history — we don't have raw click data to re-bucket precisely beyond that anyway (§3.2), so going further back wouldn't buy anything.

---

## 4. Currency Handling

### 4.1 FX rate source and granularity

**Source:** a daily-close FX rate feed — something like [openexchangerates.org](https://openexchangerates.org) or the ECB's daily reference rates — pulled once a day into `ads.dim_fx_rate`, keyed on `(rate_date, currency_code)`, priced as USD per unit of currency. USD is the pivot, so any pair converts as a cross-rate: `amount_target = amount_source * (usd_per_unit[source] / usd_per_unit[target])`. That avoids storing every currency pair (n²) — we just store n rates against one pivot and derive the rest.

**Why daily, not intraday:** the ad platforms themselves report spend in the account's native currency at daily granularity given our extraction cadence, and none of the D2C reporting use cases here actually need intraday FX precision. Daily-close is standard practice for ad-spend reporting anyway — it's what the platforms' own currency-converted reports use too. If a customer ever genuinely needed intraday FX, only the rate table's grain would need to change; nothing else in the pipeline would.

**Which rate_date to use for a given row:** the FX rate for the row's *source* `report_date` (the account-local day), not the converted `reporting_day`. Spend happened in the account's currency on the account's calendar day, so that's the economically correct rate — and it's also the only rate guaranteed to already exist (we never look up a future-dated rate; `pipeline._to_fact_row` enforces this with `fx_rate_date = min(report_date, today)`).

### 4.2 Store source, reporting, or both

**Both.** This is one place I was deliberate rather than just picking one and moving on. `fact_campaign_performance` stores:
- `spend_minor_source` + `source_currency` — what the platform actually billed, in the account's native currency
- `spend_minor_reporting` + `reporting_currency` — the converted number, what the customer actually sees
- `fx_rate_used` + `fx_rate_date` — a full audit trail of exactly which rate produced the converted number

Storing only the converted number would make reconciliation against the platform UI (§5.3) impossible without re-deriving the original from a rate that might have since been restated (§4.3). Storing only the source number pushes conversion into every query instead, which is both slower and a correctness risk — nothing stops two dashboards from converting with different rates. Storing both, plus the rate itself, costs three extra columns and buys full auditability, which felt like an easy trade.

### 4.3 Historical FX restatements

Daily-close rate providers occasionally restate a historical rate — fixing a feed error, or switching vendors. Because `fx_rate_used` gets captured per fact row at write time, a later restatement of `dim_fx_rate` does not retroactively change already-written fact rows, by design. If we ever wanted restated history to actually propagate, that's an explicit, auditable backfill — re-running the affected `rate_date` range through the pipeline — using the same mechanism as §3.4, not an automatic cascade. I'd only bother triggering this above some materiality threshold (say, >0.5% rate change); daily FX noise below that isn't worth a backfill.

### 4.4 Customer switches EUR → USD

Same mechanism as a `reporting_timezone` change (§3.4): a row in `dim_customer_setting_history` (`attribute='reporting_currency'`), `backfill_status='pending'`, and a backfill job that re-converts historical `spend_minor_source` — which never changes, it's the platform's native-currency truth — into the new `reporting_currency`, using each row's original `fx_rate_date`. This one's actually simpler than the timezone case: a currency backfill only touches `spend_minor_reporting`/`reporting_currency`/`fx_rate_used` on existing rows (a plain `UPDATE`, not a re-bucket), since `reporting_day` and the campaign/account keys never change. No orphaned-row problem like §3.4's bucket shift.

### 4.5 Rounding

All monetary math runs in integer minor units (cents) end to end, never floats, and rounding happens exactly once — at the moment of currency conversion — using `ROUND_HALF_EVEN` (banker's rounding, see [`currency.py`](../src/adpipeline/currency.py)). Naive "always round up" rounding introduces a small systematic upward bias that compounds across millions of daily rows; banker's rounding stays unbiased in aggregate. It's a small enough detail that I half expect it to come up in the follow-up discussion, so I made sure the test suite actually proves it (`test_currency.py::test_rounding_uses_banker_rounding_not_always_up`) instead of just asserting it in a comment.

---

## 5. Data Accuracy

### 5.1 Attribution windows without re-pulling everything

All three platforms revise conversion (and occasionally click) numbers for up to 7–28 days after the report date, as more attributed conversions land within the click's attribution window. Re-pulling a customer's entire history on every run to catch these revisions doesn't scale, and it isn't necessary anyway — spend and impressions are essentially final same-day; it's conversions that drift.

**Approach: a bounded rolling reconciliation window, not a full re-pull.**
- Every daily run extracts yesterday plus a trailing N-day window (N = the platform's max attribution window — 7 for Meta by default, up to 28 for extended attribution) for any account where a row in that window is still `is_final = false`.
- `ads.fact_campaign_performance.is_final` flips once `days_old >= attribution_window_days` *and* the platform's own finality signal agrees where one exists (§1.3 — Meta gives an explicit flag; Google Ads has no such flag, so we infer finality purely from row age).
- The partial index `ix_fact_not_final` (§2.1) is what makes "which rows still need reconciliation" a cheap, targeted query rather than a full-table scan — that's the whole reason it exists.
- Once a row is `is_final = true`, it drops out of future reconciliation passes, which keeps that work bounded to a small, roughly constant window (attribution window × active accounts) regardless of how much history has piled up.

### 5.2 Reconciliation against platform UI

I'd add a lightweight, scheduled reconciliation check — not part of the core hot path — that periodically re-pulls a small sample (random or targeted) of `(account, day)` pairs already marked `is_final` and diffs them against what's stored. A nonzero diff on a row we thought was final is itself worth an alert: it means either a platform-side restatement happened outside the expected attribution window (rare, but Meta does occasionally issue data corrections), or our finality heuristic is simply wrong for that platform or account. This check stays cheap — small sample, low frequency, daily or weekly — and it's the mechanism that would catch the "customer says yesterday's numbers look wrong" scenario in §6.3 before the customer ever files the ticket.

### 5.3 Idempotency on worker crash

The `postgres_writer.py` docstring covers this mechanically; here's the design-level version. A worker can crash after fetching data but before writing, mid-write, or after writing but before it manages to mark `extraction_run.status = succeeded`. In every one of those cases, simply re-running the same task has to be safe and land on the same end state — no manual cleanup, no "did that partial write actually commit?" investigation.

Three things work together to make that true:
1. `ux_fact_natural_key` is a DB-level unique constraint, not an application-level check. The application never has to ask "does this row already exist?" — that check-then-act pattern races under concurrent workers — the constraint just makes duplication structurally impossible.
2. `INSERT ... ON CONFLICT DO UPDATE` is one atomic statement per row, not a `SELECT` followed by a branch into `INSERT` or `UPDATE`.
3. Each batch commits inside a single transaction (`with self.conn.transaction():` in `PostgresWriter`). A crash mid-batch leaves the fact table exactly as it was before the batch started, and the next attempt just reprocesses the whole date range from scratch — safe because it's idempotent, rather than trying to resume from "wherever it crashed," which would be fragile and not worth the complexity at this data volume.

This is tested directly in `tests/test_idempotency.py` — double-write, revised-conversions-on-rerun, and non-colliding-key cases — against an in-memory double that mirrors the real SQL's conflict-target semantics, plus the same assertion run against a live Postgres in `tests/test_idempotency_integration.py`.

---

## 6. Operations

### 6.1 Per-account rate limits across 1,000 customers

All three platforms enforce rate limits per ad account — Meta with a compute-time budget per account, Google Ads with an operations/day quota per developer token *and* per account, TikTok with requests/minute per advertiser account — rather than globally per API key, which works in our favor since the natural sharding of work (one worker task per account, §1.2) is already rate-limit-isolated. At 1,000 customers, ~2.5 accounts each, across 3 platforms — roughly 7,500 accounts — the real risk isn't hitting any single account's limit (a daily-refresh account only makes a handful of calls a day); it's thundering-herd: every daily job kicking off at the same wall-clock minute and momentarily overwhelming a shared resource, like our own outbound connection pool or a platform's app-wide quota sitting on top of the per-account one (Google Ads has exactly this). The mitigation is staggering job start times — jitter across a window rather than everything firing at `00:00` — with the per-account token bucket (§1.3) as a second line of defense that backs off gracefully even if the stagger isn't perfect.

### 6.2 Expired tokens and revoked access

This surfaces as a first-class `AuthError` in the connector layer, and it's never silently retried — `BaseConnector._fetch_with_retry` re-raises it immediately instead of treating it like a transient failure. When a worker hits this:
1. `dim_ad_account.token_status` flips to `expired` or `revoked`.
2. The account gets excluded from future extraction attempts until `token_status` returns to `ok` — no point burning retry budget on a credential that's already dead.
3. An alert goes to the customer-facing team, since reconnecting the ad account is a customer action item, not an engineering incident. Separately, a lower-urgency internal metric tracks how many accounts have sat in `expired`/`revoked` for more than 24 hours, as a churn-risk signal — a customer who doesn't reconnect within a day or two is at real risk of quietly losing data continuity.

### 6.3 Observability, alerting, and an on-call walkthrough

**Metrics/dashboards**, per account and rolled up per platform:
- extraction success/failure rate, rows written, retries used, pages fetched (already returned by `ExtractionResult` and logged per run in `extraction_run`)
- count and age distribution of `is_final=false` rows, to see whether reconciliation is keeping up or falling behind
- distribution of `token_status` across accounts
- FX rate freshness — did today's daily-close rate actually land before the first extraction run needed it?

**Alerts:** `extraction_run.status = failed` beyond a retry budget; any account with no successful pull in over 36h (daily) or 3h (hourly); a spike in `AuthError`s (could be a platform outage getting misclassified as auth — worth checking specifically); a missing FX rate for today.

**On-call walkthrough — "customer says yesterday's numbers look wrong":**
1. Look up the customer's `reporting_timezone`/`reporting_currency` and make sure "yesterday" means the same calendar day to them as it does to me — `(now() AT TIME ZONE reporting_timezone)::date - 1`. The sample query in §2.3 is literally the first thing I'd run.
2. Check `is_final` for that customer's rows on that day. If it's false, this is very likely not a bug at all — the attribution window just hasn't closed yet (§5.1). I'd expect this to be the most common "wrong number" report by far, and the right response is customer communication ("still finalizing, check back in N days"), not an engineering fix. That's exactly why `is_final`/`all_rows_final` is surfaced in the sample query, and it should be surfaced on the customer-facing dashboard too, not kept internal.
3. If it's already `is_final = true` and the customer still disputes it: pull up `stg_campaign_daily.raw_payload` for that account/day and compare it against what the platform's UI shows right now. A live mismatch means the platform revised data outside the window we expected (rare, but see §5.2) — that's a genuine gap, and the fix is a targeted re-extraction of that one `(account, day)`.
4. If the raw payload matches the platform but the fact row doesn't match the raw payload, the bug is somewhere in normalize/convert/upsert — wrong FX rate applied, a tz bucketing slip, that sort of thing. `timezone_utils.py` and `currency.py` are pure functions on purpose, specifically so I can re-run them by hand against the disputed row in a Python shell mid-incident.
5. Check the `extraction_run` history for that account/day for any `partial`/`failed` runs that might point to a botched retry.

### 6.4 Backpressure / cost containment

If a platform starts consistently 429/613-ing us despite backoff — say a rate-limit policy tightened on their end — the token-bucket capacity per account is a single config value to turn down globally. That's deliberate: an incident response should be a config push, not a code change.

---

## 7. Cost

**API quotas:** at 1,000 customers, ~7,500 accounts, one call cluster (a handful of paginated requests) per account per day, or per hour for the hourly cohort, we land at low tens of thousands of API calls a day — comfortably inside normal developer-tier quotas for all three platforms. This isn't the bottleneck at this scale, and I wouldn't spend effort optimizing call count until well past 1,000 customers.

**Compute:** short-lived, I/O-bound extraction tasks — a modest, fixed Airflow worker pool (20–50 concurrent task slots) clears ~7,500 daily tasks plus the hourly cohort's extra volume without much trouble. Cost here is dominated by orchestrator/pod-scheduling overhead, not raw CPU.

**Postgres growth:** roughly 4.5M fact rows a year at 1,000 customers (§2.1) — a few GB a year including indexes, trivial for a managed Postgres instance. Staging (`stg_campaign_daily`) is the bigger volume driver because of the `raw_payload JSONB` column, but the 90-day retention (§2.2) keeps that bounded rather than growing forever.

**First place I'd cut cost at 1,000 customers:** the `raw_payload JSONB` column in staging. It's genuinely valuable for debugging — §6.3's step 3 depends on it directly — but it's also by far the largest storage line relative to how often it actually earns its keep. I'd keep full 90-day retention for accounts with a recent dispute or anomaly flag, and truncate more aggressively (down to just the fields we actually parse, or a shorter 14–30 day window) for accounts with a clean track record. A targeted, evidence-based policy rather than a blanket cut — I don't want to sacrifice the one debugging tool I actually rely on (§6.3) just to save a bit of storage.

---

## 8. What I'd do next if I had more time

- Hourly-grain extraction for the `refresh_cadence = hourly` cohort, to shrink the timezone-bucketing approximation in §3.2 from day-level down to hour-level.
- An actual reconciliation job (§5.2), rather than just describing one. I scoped implementation time for this take-home toward the core extract → normalize → convert → upsert path and its tests, and treated reconciliation as designed-but-not-built.
- Meta and TikTok connectors, on the exact same `BaseConnector` contract as the Google Ads implementation here — the whole point of §1.3's design is that this should be a small diff, not a rewrite.
- Cold-tier storage migration tooling for `fact_campaign_performance` partitions older than ~13 months (§7), once real Postgres growth data justifies it — building that against a guess felt premature.
- A materialized rollup table (`fact_campaign_daily_totals`, at the customer/day grain, skipping campaigns) if dashboard query latency ever actually becomes a measured problem — not worth building preemptively at the base table's current row counts.
- The ML-facing feature layer described in §9 below, once the reconciliation job and Meta/TikTok connectors are in place — it's the natural next tier once the fact table is trustworthy enough to build models on top of.

---

## 9. ML Extension / Future Use Cases

Not part of the assignment, but worth a quick note: this fact table is clean enough (one trusted row per customer/platform/account/campaign/day, with an explicit `is_final` flag) to double as a feature store later, if forecasting or anomaly detection ever becomes a priority.

```
Trusted campaign fact
        ↓
Feature engineering
        ↓
Customer/platform/day features
        ↓
Spend forecasting
Budget anomaly detection
Campaign performance prediction
        ↓
Model evaluation
        ↓
Production inference
        ↓
Monitoring / drift
```

Example features, all derivable from columns already in the schema: 7-day spend, 28-day spend, spend growth rate, CTR, CPC, conversion rate, rolling conversion trend, platform mix, campaign age, day-of-week, seasonality, lagged spend/conversions.

One caveat worth flagging up front: all feature calculations must be point-in-time correct to prevent data leakage, particularly because ad-platform attribution data can be revised after the initial reporting date (the same issue §5.1 covers for reporting — it just bites harder in a training set).
