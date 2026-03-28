from decimal import Decimal

from pydantic import BaseModel

from houseflip.models.listing import ListingType, PropertyType


class NeighborhoodStats(BaseModel):
    city: str
    neighborhood: str
    listing_type: ListingType
    property_type: PropertyType
    listing_count: int
    median_price: Decimal
    mean_price: Decimal
    stddev_price: Decimal
    median_price_per_m2: Decimal | None
    mean_price_per_m2: Decimal | None
    stddev_price_per_m2: Decimal | None


class OpportunityListing(BaseModel):
    """A listing enriched with its opportunity score relative to neighborhood peers."""

    listing_id: str
    external_id: str
    source: str
    url: str
    city: str
    neighborhood: str
    listing_type: str
    property_type: str
    price_brl: Decimal
    area_m2: Decimal | None
    price_per_m2: Decimal | None
    bedrooms: int | None
    bathrooms: int | None
    parking_spots: int | None
    title: str | None

    # Analysis fields
    z_score_price: float
    z_score_price_per_m2: float | None
    opportunity_score: float  # higher = better deal (negative z-score)
    neighborhood_median_price: Decimal
    neighborhood_median_price_per_m2: Decimal | None
    pct_below_median: float  # e.g. -15.3 means 15.3% below median
