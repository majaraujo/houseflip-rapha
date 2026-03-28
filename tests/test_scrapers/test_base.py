"""Tests for scraper models and config validation."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import pytest
from pydantic import ValidationError

from houseflip.models.listing import ListingSource, ListingType, PropertyType
from houseflip.models.scrape_config import ScrapeJob


def test_scrape_job_defaults():
    job = ScrapeJob(source=ListingSource.ZAPIMOVEIS, city="São Paulo")
    assert job.max_pages == 5
    assert job.listing_type == ListingType.SALE
    assert job.property_type == PropertyType.APARTMENT
    assert job.neighborhood is None


def test_scrape_job_max_pages_validation():
    with pytest.raises(ValidationError):
        ScrapeJob(source=ListingSource.ZAPIMOVEIS, city="São Paulo", max_pages=0)

    with pytest.raises(ValidationError):
        ScrapeJob(source=ListingSource.ZAPIMOVEIS, city="São Paulo", max_pages=51)


def test_listing_price_per_m2_computed():
    from decimal import Decimal
    from datetime import datetime, timezone
    from houseflip.models.listing import Listing

    listing = Listing(
        external_id="test-1",
        source=ListingSource.ZAPIMOVEIS,
        url="https://example.com/1",
        listing_type=ListingType.SALE,
        property_type=PropertyType.APARTMENT,
        city="São Paulo",
        neighborhood="Pinheiros",
        price_brl=Decimal("500000"),
        area_m2=Decimal("100"),
        scraped_at=datetime.now(timezone.utc),
    )

    assert listing.price_per_m2 == Decimal("5000.00")


def test_listing_price_per_m2_none_without_area():
    from decimal import Decimal
    from datetime import datetime, timezone
    from houseflip.models.listing import Listing

    listing = Listing(
        external_id="test-2",
        source=ListingSource.ZAPIMOVEIS,
        url="https://example.com/2",
        listing_type=ListingType.SALE,
        property_type=PropertyType.APARTMENT,
        city="São Paulo",
        neighborhood="Pinheiros",
        price_brl=Decimal("500000"),
        scraped_at=datetime.now(timezone.utc),
    )

    assert listing.price_per_m2 is None
