import os

from pydantic import BaseModel, Field

from houseflip.models.listing import ListingSource, ListingType, PropertyType


def _default_request_delay() -> float:
    return float(os.getenv("SCRAPER_REQUEST_DELAY_SECONDS", "1.5"))


class ScrapeJob(BaseModel):
    source: ListingSource
    city: str
    state: str = "sp"
    neighborhood: str | None = None
    listing_type: ListingType = ListingType.SALE
    property_type: PropertyType = PropertyType.APARTMENT
    max_pages: int = Field(default=5, ge=1, le=500)
    request_delay_seconds: float = Field(default_factory=_default_request_delay, ge=0.5, le=10.0)
