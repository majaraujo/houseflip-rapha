"""Orchestrates scraper runs and persists results to DuckDB."""

import asyncio
import concurrent.futures
import logging
from collections.abc import Generator

import httpx

from houseflip.models.listing import Listing
from houseflip.models.scrape_config import ScrapeJob
from houseflip.scrapers import SCRAPER_REGISTRY
from houseflip.storage.database import Database
from houseflip.storage.repository import ListingRepository

logger = logging.getLogger(__name__)


def _run_async_in_thread(coro):
    """Run a coroutine in a fresh event loop in a dedicated thread.

    This avoids RuntimeError when called from within Streamlit's own event loop.
    """
    def _target():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
            asyncio.set_event_loop(None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(_target).result()


class ScrapeService:
    def __init__(self, db: Database) -> None:
        self._repo = ListingRepository(db)

    def run(self, job: ScrapeJob) -> Generator[dict, None, None]:
        """
        Synchronous generator that drives the async scraper.
        Yields progress dicts: {"page": int, "found": int, "total": int, "listings": list}
        """
        scraper_cls = SCRAPER_REGISTRY[job.source]
        run_id = self._repo.create_scrape_run(
            source=job.source,
            city=job.city,
            neighborhood=job.neighborhood,
            listing_type=job.listing_type,
            property_type=job.property_type,
        )

        # Pre-load IDs already in the DB so the scraper can skip them and keep paginating
        existing_ids = self._repo.get_existing_ids(
            source=job.source,
            listing_type=job.listing_type,
            property_type=job.property_type,
            city=job.city,
        )
        if existing_ids:
            job = job.model_copy(update={"known_ids": frozenset(existing_ids)})

        total_found = 0
        all_listings: list[Listing] = []
        status = "done"

        async def _collect() -> list[tuple[int, list[Listing]]]:
            pages: list[tuple[int, list[Listing]]] = []
            async with scraper_cls(job) as scraper:
                page = 1
                async for batch in scraper.scrape():
                    pages.append((page, batch))
                    page += 1
            return pages

        try:
            pages = _run_async_in_thread(_collect())
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            url = str(exc.request.url)
            if status_code == 403:
                msg = f"Acesso negado (HTTP 403) — o site bloqueou a requisição. Tente novamente em alguns minutos. URL: {url}"
            elif status_code == 429:
                msg = f"Muitas requisições (HTTP 429) — aguarde alguns minutos antes de tentar novamente. URL: {url}"
            elif status_code == 404:
                msg = f"Página não encontrada (HTTP 404) — verifique cidade/bairro/estado. URL: {url}"
            else:
                msg = f"Erro HTTP {status_code} ao acessar: {url}"
            logger.error("Scraper HTTP error: %s", msg)
            self._repo.finish_scrape_run(run_id, total_found=0, status="error")
            yield {"page": 0, "found": 0, "total": 0, "listings": [], "error": msg}
            return
        except Exception as exc:
            logger.exception("Scraper failed: %s", exc)
            self._repo.finish_scrape_run(run_id, total_found=0, status="error")
            yield {"page": 0, "found": 0, "total": 0, "listings": [], "error": str(exc)}
            return

        for page_num, batch in pages:
            if batch:
                self._repo.upsert_listings(batch, run_id, city_override=job.city)
                all_listings.extend(batch)
                total_found += len(batch)
            yield {
                "page": page_num,
                "found": len(batch),
                "total": total_found,
                "listings": [l.model_dump() for l in batch],
                "error": None,
            }

        self._repo.finish_scrape_run(run_id, total_found=total_found, status=status)
        logger.info("Scrape run %s complete: %d listings", run_id, total_found)
