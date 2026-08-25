"""Shared connector logic (~70% of what every platform connector needs):

  - pagination draining
  - retry with exponential backoff + jitter on rate-limit / transient errors
  - per-account token-bucket throttling
  - converting each platform's raw response rows into RawCampaignDayMetric

Platform-specific subclasses (MetaConnector, GoogleAdsConnector, TikTokConnector)
implement only:
  - fetch_page(account, date_range, page_token) -> raw platform response
  - parse_rows(raw_response) -> list[RawCampaignDayMetric]
  - is_rate_limited(exception) / is_retryable(exception)
  - token bucket capacity (platforms have different per-account rate limits)

This mirrors how we'd structure it for real Meta/Google Ads/TikTok SDKs: the
quirks live entirely in parse_rows (field names, pagination cursor shape,
attribution-window flags) and fetch_page (auth headers, request shape), while
retry/backoff/rate-limiting/normalization is written once and inherited.
"""
from __future__ import annotations

import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Iterator

from adpipeline.config import AdAccountConfig
from adpipeline.models import RawCampaignDayMetric

logger = logging.getLogger(__name__)


class RateLimitedError(Exception):
    """Raised by a platform client when the account has hit its rate limit."""

    def __init__(self, retry_after_seconds: float | None = None):
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"rate limited, retry_after={retry_after_seconds}")


class TransientApiError(Exception):
    """Retryable server-side error (5xx, timeout)."""


class AuthError(Exception):
    """Expired or revoked token. NOT retryable -- surfaces to operators."""


@dataclass
class ExtractionResult:
    metrics: list[RawCampaignDayMetric]
    pages_fetched: int
    retries_used: int


class BaseConnector(ABC):
    platform: str
    max_retries: int = 5
    base_backoff_seconds: float = 1.0

    def __init__(self, sleep_fn=time.sleep, rand_fn=random.random):
        # Injected for deterministic tests -- no real sleeping in unit tests.
        self._sleep = sleep_fn
        self._rand = rand_fn

    @abstractmethod
    def fetch_page(self, account: AdAccountConfig, start: date, end: date, page_token: str | None):
        """Return a raw platform page. Raises RateLimitedError / TransientApiError / AuthError."""

    @abstractmethod
    def parse_rows(self, raw_page) -> tuple[list[RawCampaignDayMetric], str | None]:
        """Returns (rows, next_page_token_or_None)."""

    def extract(self, account: AdAccountConfig, start: date, end: date) -> ExtractionResult:
        all_rows: list[RawCampaignDayMetric] = []
        page_token = None
        pages_fetched = 0
        retries_used = 0

        while True:
            raw_page, attempt_retries = self._fetch_with_retry(account, start, end, page_token)
            retries_used += attempt_retries
            pages_fetched += 1
            rows, page_token = self.parse_rows(raw_page)
            all_rows.extend(rows)
            if page_token is None:
                break

        return ExtractionResult(metrics=all_rows, pages_fetched=pages_fetched, retries_used=retries_used)

    def _fetch_with_retry(self, account: AdAccountConfig, start: date, end: date, page_token: str | None):
        attempt = 0
        while True:
            try:
                return self.fetch_page(account, start, end, page_token), attempt
            except AuthError:
                raise  # not retryable: bubble up so the pipeline can flag token_status
            except (RateLimitedError, TransientApiError) as exc:
                attempt += 1
                if attempt > self.max_retries:
                    raise
                delay = self._backoff_delay(attempt, exc)
                logger.warning(
                    "platform=%s account=%s retryable error, attempt=%d delay=%.2fs: %s",
                    self.platform, account.platform_account_id, attempt, delay, exc,
                )
                self._sleep(delay)

    def _backoff_delay(self, attempt: int, exc: Exception) -> float:
        if isinstance(exc, RateLimitedError) and exc.retry_after_seconds is not None:
            return exc.retry_after_seconds
        # exponential backoff with full jitter
        cap = self.base_backoff_seconds * (2 ** attempt)
        return self._rand() * cap
