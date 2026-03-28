"""Facade for the price deviation analysis."""

from datetime import datetime

import polars as pl

from houseflip.analysis.price_deviation import PriceDeviationService
from houseflip.storage.database import Database
from houseflip.storage.repository import ListingRepository


class AnalysisService:
    def __init__(self, db: Database) -> None:
        self._db = db
        self._repo = ListingRepository(db)
        self._engine = PriceDeviationService(db)

    def get_opportunities(
        self,
        city: str | None = None,
        listing_type: str | None = None,
        property_type: str | None = None,
        neighborhood: str | None = None,
        min_listings: int = 3,
        scraped_after: datetime | None = None,
        neighborhoods: list[str] | None = None,
        area_min: float | None = None,
        area_max: float | None = None,
        sources: list[str] | None = None,
    ) -> pl.DataFrame:
        # Combine single neighborhood (drill-down) with multi-select filter
        neigh_filter = neighborhoods or ([neighborhood] if neighborhood else None)
        df = self._engine.compute_opportunities(
            city, listing_type, property_type, min_listings,
            scraped_after=scraped_after,
            neighborhoods=neigh_filter,
            area_min=area_min,
            area_max=area_max,
            sources=sources,
        )
        return df

    def get_neighborhood_summary(
        self,
        city: str | None = None,
        listing_type: str | None = None,
        property_type: str | None = None,
        min_listings: int = 3,
        scraped_after: datetime | None = None,
        neighborhoods: list[str] | None = None,
        area_min: float | None = None,
        area_max: float | None = None,
        sources: list[str] | None = None,
    ) -> pl.DataFrame:
        return self._engine.neighborhood_summary(
            city, listing_type, property_type, min_listings,
            scraped_after=scraped_after,
            neighborhoods=neighborhoods,
            area_min=area_min,
            area_max=area_max,
            sources=sources,
        )

    def available_cities(self) -> list[str]:
        return self._repo.distinct_cities()

    def available_sources(self) -> list[str]:
        return self._repo.distinct_sources()

    def available_neighborhoods(self, city: str | None = None) -> list[str]:
        return self._repo.distinct_neighborhoods(city)

    def total_listings(self) -> int:
        return self._repo.total_listings()

    def recent_scrape_runs(self, limit: int = 10) -> list[dict]:
        return self._repo.list_scrape_runs(limit)
