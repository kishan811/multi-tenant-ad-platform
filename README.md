# Ad Platform Pipeline (Google Ads) — Take-Home Implementation

This is the "40%" implementation deliverable: a Google Ads connector that pulls campaign-level
daily metrics, normalizes them for timezone and currency, and writes them into Postgres
idempotently. The reasoning behind the full multi-platform system — Meta, Google Ads, and
TikTok, orchestrated on Airflow — is in [`DESIGN.md`](DESIGN.md).

I mocked the Google Ads API rather than hitting the real thing
([`mock_google_ads_api.py`](src/adpipeline/mock_google_ads_api.py)),
since the assignment says that's fine. The mock simulates pagination (`nextPageToken`),
per-account rate limiting (`RESOURCE_EXHAUSTED`, with no retry-after hint), expired/revoked
OAuth credentials, and conversion-lag revisions on re-pull.

There's also a rendered walkthrough — **[`design-walkthrough.html`](design-walkthrough.html)**,
just open it in a browser — that goes point-by-point through what the assignment asked for,
what I decided, why, plus the next steps and the bugs I ran into building this. It's meant to
sit alongside this README and `DESIGN.md`, not replace either one.

## Project layout

```
config/customers.yaml              demo customer/ad-account config (stands in for dim_customer/dim_ad_account)
migrations/001_init.sql            full Postgres schema (staging, fact, dims, partitioning, indexes)
dags/google_ads_daily.py           illustrative Airflow DAG (dynamic task mapping over accounts) -- see caveat below
src/adpipeline/
  config.py                        load + validate customer config (tz/currency validation)
  timezone_utils.py                account-day -> customer reporting-day bucketing, DST helpers
  currency.py                      minor-unit currency conversion via USD pivot, banker's rounding
  models.py                        RawCampaignDayMetric — the shared connector output contract
  base_connector.py                shared pagination/retry/backoff logic (~70% shared across platforms)
  google_ads_connector.py          Google Ads-specific field mapping + error classification
  mock_google_ads_api.py           fixture-driven fake of the Google Ads Search API
  postgres_writer.py               idempotent upsert (INSERT ... ON CONFLICT DO UPDATE)
  pipeline.py                      orchestrates one customer's extraction end to end
  cli.py                           `python -m adpipeline.cli` entrypoint
tests/                              unit tests (see "Running tests" below)
docker-compose.yml                  local Postgres for the real DB run / integration test
design-walkthrough.html             point-by-point assignment walkthrough, next steps, bugs found
```

## Setup

Needs Python 3.11+.

```bash
cd ad-pipeline-takehome
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

## Running the pipeline locally against a real Postgres

```bash
docker compose up -d postgres
# wait a few seconds for the healthcheck, then confirm the schema loaded:
docker compose logs postgres | grep "CREATE TABLE" | tail -5
```

```bash
export DATABASE_URL=postgresql://adpipeline:adpipeline@localhost:5433/adpipeline
python -m adpipeline.cli --start 2026-08-15 --end 2026-08-15 --today 2026-08-16
```

`--today` overrides "now" so the mock's reporting-lag simulation and FX-rate lookup stay
deterministic for a demo run — leave it off to use the real current date.

Take a look at what landed:

```bash
docker exec -it $(docker compose ps -q postgres) psql -U adpipeline -d adpipeline \
  -c "SELECT customer_id, ad_account_id, reporting_day, spend_minor_reporting, reporting_currency, conversions, is_final FROM ads.fact_campaign_performance ORDER BY reporting_day;"
```

Run the same command again — the row count shouldn't move, that's the idempotent upsert doing
its job. Run it again with a much later `--today` (say `--today 2026-09-20`, past the 30-day
Google Ads conversion-lag window this pipeline uses) and you should see `conversions` and
`is_final` update in place on the same rows, still with no new rows created.

Tear it down with `docker compose down -v` when you're done.

## Running tests

```bash
pytest -q
```

31 unit tests, no external dependencies needed — timezone edge cases, currency conversion,
idempotent upserts against an in-memory Postgres-semantics double, the pipeline's finality
logic, and API error handling (rate limits with no retry-after hint, retries, pagination,
expired/revoked OAuth credentials, conversion-lag maturity).

There's one more test that's skipped unless a live Postgres is actually running:

```bash
docker compose up -d postgres
export DATABASE_URL=postgresql://adpipeline:adpipeline@localhost:5433/adpipeline
pytest -q tests/test_idempotency_integration.py
```

It re-checks the same idempotency assertion as `tests/test_idempotency.py`, but against the
real `INSERT ... ON CONFLICT` SQL in `migrations/001_init.sql` instead of the in-memory double —
worth running once to make sure the fake and the real thing actually agree.

## Full end-to-end verification checklist

Everything above in one sequence, if you want to convince yourself the whole thing actually
works rather than take my word for it. Bash on the left, PowerShell on the right — pick your
shell and run top to bottom.

**1. Run the unit tests (no Docker needed yet)**

```bash
pytest -q
```
```powershell
pytest -q
```
Expect `30 passed, 1 skipped`.

**2. Start Postgres and confirm the schema loaded**

```bash
docker compose up -d postgres
docker compose logs postgres | grep "CREATE TABLE"
```
```powershell
docker compose up -d postgres
docker compose logs postgres | Select-String "CREATE TABLE"
```
Expect 13 `CREATE TABLE` lines.

**3. Point the pipeline at that Postgres**

```bash
export DATABASE_URL=postgresql://adpipeline:adpipeline@localhost:5433/adpipeline
```
```powershell
$env:DATABASE_URL = "postgresql://adpipeline:adpipeline@localhost:5433/adpipeline"
```

**4. Run the pipeline twice — row count must not change (idempotency)**

```bash
python -m adpipeline.cli --start 2026-08-15 --end 2026-08-15 --today 2026-08-16
python -m adpipeline.cli --start 2026-08-15 --end 2026-08-15 --today 2026-08-16
```
(same command, either shell)

**5. Check what actually landed**

```bash
docker exec -it $(docker compose ps -q postgres) psql -U adpipeline -d adpipeline -c "SELECT customer_id, ad_account_id, reporting_day, spend_minor_reporting, reporting_currency, conversions, is_final FROM ads.fact_campaign_performance ORDER BY reporting_day;"
```
(same command, either shell — `$(...)` works in both)

Expect exactly 4 rows, `is_final = f`.

**6. Advance "today" past the 30-day conversion-lag window — rows update in place, still no duplicates**

```bash
python -m adpipeline.cli --start 2026-08-15 --end 2026-08-15 --today 2026-09-20
docker exec -it $(docker compose ps -q postgres) psql -U adpipeline -d adpipeline -c "SELECT count(*) AS total_rows, sum(case when is_final then 1 else 0 end) AS final_rows FROM ads.fact_campaign_performance;"
```
(same command, either shell)

Expect `total_rows = 4, final_rows = 4`.

**7. Run the live-Postgres integration test**

```bash
pytest -q tests/test_idempotency_integration.py
```
```powershell
pytest -q tests/test_idempotency_integration.py
```
Expect `1 passed` now that `DATABASE_URL` is set.

**8. Tear down**

```bash
docker compose down -v
```
```powershell
docker compose down -v
```

If every step matches what's described, the pipeline, schema, and idempotency/reconciliation
logic all work end-to-end.

## Assumptions I made for this take-home

- **Google Ads is the platform I implemented.** The design doc covers all three (Meta, Google
  Ads, TikTok); the base connector contract (`base_connector.py`) is written so adding Meta or
  TikTok later is a small addition, not a rewrite.
- **The API is mocked**, which the assignment explicitly allows — no real ad credentials spent.
  The fixture reproduces the quirks the assignment specifically asks about: pagination,
  rate-limit responses (with no retry-after hint, unlike Meta), and conversion-lag revisions.
  (`mock_google_ads_api.py`'s docstring spells out exactly what's simulated and how it differs
  from Meta.)
- **`dim_customer`/`dim_ad_account` are seeded from `config/customers.yaml`** instead of a real
  onboarding flow, which doesn't exist for this exercise. `cli.py`'s `_ensure_dims` upserts them
  idempotently on every run, so the whole demo is one command.
- **FX rates are a small hardcoded table** (`cli.py::_DEMO_FX_RATES`) rather than a real
  daily-close feed. `DESIGN.md` §4 covers what the real source would be (openexchangerates.org
  or the ECB daily rates) and how `dim_fx_rate` would actually get populated in production.
- **The conversion-lag window is fixed at 30 days** (Google Ads' own default UI reporting
  window) instead of being configurable per platform or account — `pipeline.py`'s
  `MAX_ATTRIBUTION_WINDOW_DAYS` is the one constant to change if you want it shorter or longer.
- **Google Ads doesn't give us a "this row is final" flag** the way Meta's `date_stop_is_final`
  does — `GoogleAdsConnector` always defers to the pipeline, and finality ends up being purely a
  function of row age (`pipeline.py::_to_fact_row`). That's a genuine accuracy trade-off, not
  something I skipped out of laziness — see `DESIGN.md` §5.1 and the walkthrough for the reasoning.
- **Reconciliation against the platform UI (`DESIGN.md` §5.2) is designed, not built.** I scoped
  it out to keep the 6–10 hour implementation window focused on the core extract → normalize →
  convert → upsert path and its tests — in the spirit of the assignment's own "thoughtful
  incompleteness beats rushed completeness."
- **`dags/google_ads_daily.py` is illustrative, not a tested production DAG.** It's real Airflow
  TaskFlow/dynamic-task-mapping syntax calling the exact same `run_customer_extraction()` that
  `cli.py` uses, so it's not a second implementation of the pipeline — but it's never been run
  against an actual Airflow scheduler (that would need real Airflow Connections and Pools
  configured, which is out of scope for this exercise). Treat it as "here's what the DAG would
  look like," not "this DAG has been verified end to end."

## What I'd build next

`DESIGN.md` §8 and the walkthrough's "Next steps" section both have the full list — the
reconciliation job first, then Meta/TikTok connectors, hourly-grain extraction, and cold-storage
partition tiering, roughly in that order of priority.
