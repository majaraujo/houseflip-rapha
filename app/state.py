"""Typed session state keys to prevent typo bugs."""

from typing import TypedDict


class AppState(TypedDict, total=False):
    last_scrape_run_id: str
    last_scrape_listings: list[dict]
    last_scrape_total: int
    analysis_city: str
    analysis_listing_type: str
    analysis_property_type: str
    analysis_neighborhood: str | None
