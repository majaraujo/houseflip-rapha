"""Tests for PriceDeviationService."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from houseflip.analysis.price_deviation import PriceDeviationService
from houseflip.models.listing import Listing, ListingSource, ListingType, PropertyType


def _make_listing(external_id: str, price: float, area: float, neighborhood: str) -> Listing:
    return Listing(
        external_id=external_id,
        source=ListingSource.ZAPIMOVEIS,
        url=f"https://example.com/{external_id}",
        listing_type=ListingType.SALE,
        property_type=PropertyType.APARTMENT,
        city="São Paulo",
        neighborhood=neighborhood,
        price_brl=Decimal(str(price)),
        area_m2=Decimal(str(area)),
        scraped_at=datetime.now(timezone.utc),
    )


def test_opportunity_score_cheaper_listing_scores_higher():
    """A listing priced 30% below average should score higher than one priced at average."""
    # Setup: 5 listings at ~500k, one at 350k (cheap outlier)
    listings = [
        _make_listing("1", 500_000, 80, "Pinheiros"),
        _make_listing("2", 510_000, 80, "Pinheiros"),
        _make_listing("3", 490_000, 80, "Pinheiros"),
        _make_listing("4", 505_000, 80, "Pinheiros"),
        _make_listing("5", 350_000, 80, "Pinheiros"),  # cheap outlier
    ]

    rows = [
        (
            f"id-{l.external_id}", l.external_id, l.source, l.url,
            l.listing_type, l.property_type, l.city, l.neighborhood,
            float(l.price_brl), float(l.area_m2),
            float(l.price_per_m2) if l.price_per_m2 else None,
            l.bedrooms, l.bathrooms, l.parking_spots, l.title,
        )
        for l in listings
    ]

    mock_result = MagicMock()
    mock_result.fetchall.return_value = rows
    mock_db = MagicMock()
    mock_db.execute.return_value = mock_result

    service = PriceDeviationService(mock_db)
    df = service.compute_opportunities()

    assert not df.is_empty()
    # The cheapest listing (external_id="5") should be ranked first
    assert df["external_id"][0] == "5"
    # Its opportunity score should be the highest (most positive)
    assert df["opportunity_score"][0] > df["opportunity_score"][-1]


def test_empty_database_returns_empty_dataframe():
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_db = MagicMock()
    mock_db.execute.return_value = mock_result

    service = PriceDeviationService(mock_db)
    df = service.compute_opportunities()

    assert df.is_empty()
