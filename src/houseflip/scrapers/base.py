"""Abstract base scraper with httpx + parsel + tenacity retry logic."""

import asyncio
import os
import random
import unicodedata
import urllib.parse
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from typing import ClassVar

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from houseflip.models.listing import Listing, ListingSource
from houseflip.models.scrape_config import ScrapeJob

_TIMEOUT_SECONDS = float(os.getenv("SCRAPER_TIMEOUT_SECONDS", "30.0"))
_MAX_RETRIES = int(os.getenv("SCRAPER_MAX_RETRIES", "3"))


def _build_request_url(url: str) -> str:
    """Wrap URL through ScraperAPI if a key is configured, otherwise return as-is.

    Key is read lazily so it picks up values injected into os.environ after import.
    """
    key = os.getenv("SCRAPERAPI_KEY")
    if not key:
        return url
    return f"http://api.scraperapi.com?api_key={key}&url={urllib.parse.quote_plus(url)}"

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]


def slugify(text: str) -> str:
    """Lowercase, remove accents, replace spaces with hyphens."""
    normalized = unicodedata.normalize("NFD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return ascii_text.lower().replace(" ", "-")


def _is_transient_error(exc: BaseException) -> bool:
    """Return True only for errors worth retrying (network or server-side transient)."""
    if isinstance(exc, httpx.HTTPStatusError):
        # Retry on rate-limit and server errors; do NOT retry 4xx client errors
        return exc.response.status_code in (429, 500, 502, 503, 504)
    return isinstance(exc, (httpx.TransportError, httpx.TimeoutException))


_BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    # Omit Accept-Encoding — let httpx negotiate automatically (avoids Brotli issues)
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}


class BaseScraper(ABC):
    source: ClassVar[ListingSource]

    def __init__(self, job: ScrapeJob) -> None:
        self.job = job
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "BaseScraper":
        headers = {**_BASE_HEADERS, "User-Agent": random.choice(_USER_AGENTS)}
        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=_TIMEOUT_SECONDS,
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Use scraper as async context manager")
        return self._client

    @abstractmethod
    def _build_url(self, page: int) -> str:
        """Build the listing search URL for a given page number."""

    @abstractmethod
    def _parse_listings(self, html: str) -> list[Listing]:
        """Parse HTML/JSON and return a list of Listing objects."""

    @abstractmethod
    def _has_next_page(self, html: str, page: int) -> bool:
        """Return True if there are more pages to scrape."""

    @retry(
        stop=stop_after_attempt(_MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(_is_transient_error),
        reraise=True,
    )
    async def _fetch_page(self, url: str) -> str:
        response = await self.client.get(_build_request_url(url))
        response.raise_for_status()
        return response.text

    async def scrape(self) -> AsyncGenerator[list[Listing], None]:
        """Async generator that yields a list of listings per page."""
        seen_ids: set[str] = set()

        for page in range(1, self.job.max_pages + 1):
            url = self._build_url(page)
            html = await self._fetch_page(url)
            listings = self._parse_listings(html)

            if not listings:
                break

            # Stop if the entire page consists of already-seen listings
            # (sites that don't support SSR pagination return the same page repeatedly)
            new_listings = [l for l in listings if l.external_id not in seen_ids]
            if not new_listings:
                logger.debug("Page %d returned only duplicates — stopping early.", page)
                break

            for l in new_listings:
                seen_ids.add(l.external_id)

            yield new_listings

            if not self._has_next_page(html, page):
                break

            # Jittered delay to avoid rate limiting
            delay = self.job.request_delay_seconds + random.uniform(-0.3, 0.5)
            await asyncio.sleep(max(0.5, delay))
