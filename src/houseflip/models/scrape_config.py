import os

from pydantic import BaseModel, Field

from houseflip.models.listing import ListingSource, ListingType, PropertyType


def _default_request_delay() -> float:
    try:
        val = float(os.getenv("SCRAPER_REQUEST_DELAY_SECONDS", "1.5"))
    except (ValueError, TypeError):
        val = 1.5
    return max(0.5, min(val, 10.0))  # clamp to valid range


class ScrapeJob(BaseModel):
    source: ListingSource
    city: str
    state: str = "sp"
    neighborhood: str | None = None
    listing_type: ListingType = ListingType.SALE
    property_type: PropertyType = PropertyType.APARTMENT
    max_pages: int = Field(default=50, ge=1, le=1000)
    request_delay_seconds: float = Field(default_factory=_default_request_delay)
