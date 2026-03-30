"""QuintoAndar scraper — uses internal JSON search API (POST)."""

import logging
import os
import urllib.parse
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from houseflip.models.listing import Listing, ListingSource, ListingType, PropertyType
from houseflip.models.scrape_config import ScrapeJob
from houseflip.scrapers.base import BaseScraper, slugify

logger = logging.getLogger(__name__)

API_URL = "https://apigw.prod.quintoandar.com.br/house-listing-search/v2/search/list"
PAGE_SIZE = 12

_BUSINESS_CONTEXT = {
    ListingType.SALE: "SALE",
    ListingType.RENT: "RENT",
}

_HOUSE_TYPES = {
    PropertyType.APARTMENT: "APARTMENT",
    PropertyType.HOUSE: "HOUSE",
    PropertyType.LOT: "LOT",
    PropertyType.COMMERCIAL: "COMMERCIAL",
}

# Coordinates for major Brazilian cities
_CITY_COORDS: dict[str, dict] = {
    "sao-paulo": {
        "lat": -23.55052, "lng": -46.633309,
        "viewport": {"north": -23.41968608051386, "south": -23.68122383243045, "east": -46.55194150732421, "west": -46.71467649267577},
    },
    "rio-de-janeiro": {
        "lat": -22.9068, "lng": -43.1729,
        "viewport": {"north": -22.75, "south": -23.07, "east": -43.09, "west": -43.80},
    },
    "belo-horizonte": {
        "lat": -19.9167, "lng": -43.9345,
        "viewport": {"north": -19.79, "south": -20.04, "east": -43.86, "west": -44.07},
    },
    "curitiba": {
        "lat": -25.4284, "lng": -49.2733,
        "viewport": {"north": -25.33, "south": -25.53, "east": -49.19, "west": -49.38},
    },
    "porto-alegre": {
        "lat": -30.0346, "lng": -51.2177,
        "viewport": {"north": -29.94, "south": -30.22, "east": -51.10, "west": -51.28},
    },
}
_DEFAULT_COORDS = _CITY_COORDS["sao-paulo"]


class QuintoAndarScraper(BaseScraper):
    source = ListingSource.QUINTOANDAR

    def _build_url(self, page: int) -> str:
        # Not used — this scraper overrides scrape() directly
        return API_URL

    def _parse_listings(self, html: str) -> list[Listing]:
        # Not used — this scraper overrides scrape() directly
        return []

    def _has_next_page(self, html: str, page: int) -> bool:
        # Not used — this scraper overrides scrape() directly
        return False

    def _build_payload(self, offset: int) -> dict:
        city_slug = slugify(self.job.city)
        state = self.job.state.lower()
        coords = _CITY_COORDS.get(city_slug, _DEFAULT_COORDS)

        if self.job.neighborhood:
            neigh_slug = slugify(self.job.neighborhood)
            slug = f"{neigh_slug}-{city_slug}-{state}-brasil"
        else:
            slug = f"{city_slug}-{state}-brasil"

        return {
            "context": {"mapShowing": False, "listShowing": True, "isSSR": False},
            "fields": [
                "id", "coverImage", "rent", "totalCost", "salePrice",
                "iptuPlusCondominium", "area", "address", "regionName", "city",
                "type", "forRent", "forSale", "isPrimaryMarket",
                "bedrooms", "parkingSpaces", "suites", "bathrooms",
                "neighbourhood", "categories", "isFurnished",
                "installations", "amenities",
            ],
            "filters": {
                "businessContext": _BUSINESS_CONTEXT[self.job.listing_type],
                "blocklist": [],
                "selectedHouses": [],
                "availability": "ANY",
                "occupancy": "ANY",
                "enableFlexibleSearch": True,
                "categories": [],
                "partnerIds": [],
                "houseSpecs": {
                    "houseTypes": [_HOUSE_TYPES.get(self.job.property_type, "APARTMENT")],
                    "area": {"range": {}},
                    "bedrooms": {"range": {}},
                    "bathrooms": {"range": {}},
                    "parkingSpace": {"range": {}},
                    "suites": {"range": {}},
                    "amenities": [],
                    "installations": [],
                },
                "location": {
                    "coordinate": {"lat": coords["lat"], "lng": coords["lng"]},
                    "countryCode": "BR",
                    "neighborhoods": [],
                    "viewport": coords["viewport"],
                },
                "priceRange": [],
                "specialConditions": [],
                "excludedSpecialConditions": [],
            },
            "locationDescriptions": [{"description": slug}],
            "pagination": {"pageSize": PAGE_SIZE, "offset": offset},
            "slug": slug,
            "sorting": {"criteria": "RELEVANCE", "order": "DESC"},
            "topics": [],
        }

    def _parse_item(self, hit: dict) -> Listing | None:
        try:
            # Elasticsearch-style: data lives in _source
            house = hit.get("_source", hit)

            external_id = str(house.get("id", hit.get("_id", "")))
            if not external_id:
                return None

            url = f"https://www.quintoandar.com.br/imovel/{external_id}"

            is_sale = house.get("forSale", False)
            listing_type = ListingType.SALE if is_sale else ListingType.RENT

            price_raw = house.get("salePrice") if is_sale else house.get("rent")
            try:
                price = Decimal(str(price_raw))
            except (InvalidOperation, TypeError):
                return None
            if price <= 0:
                return None

            area_raw = house.get("area")
            try:
                area = Decimal(str(area_raw)) if area_raw else None
            except (InvalidOperation, TypeError):
                area = None

            bedrooms = house.get("bedrooms")
            bathrooms = house.get("bathrooms")
            parking = house.get("parkingSpaces")

            neighborhood = house.get("neighbourhood") or house.get("regionName") or ""
            city = house.get("city") or self.job.city
            address = house.get("address") or ""

            house_type = house.get("type", "").upper()
            if "CASA" in house_type or house_type == "HOUSE":
                prop_type = PropertyType.HOUSE
            else:
                prop_type = self.job.property_type

            return Listing(
                external_id=external_id,
                source=ListingSource.QUINTOANDAR,
                url=url,
                listing_type=listing_type,
                property_type=prop_type,
                city=city,
                neighborhood=neighborhood,
                street=address or None,
                price_brl=price,
                area_m2=area,
                bedrooms=int(bedrooms) if bedrooms is not None else None,
                bathrooms=int(bathrooms) if bathrooms is not None else None,
                parking_spots=int(parking) if parking is not None else None,
                scraped_at=datetime.now(timezone.utc),
            )
        except Exception:
            logger.exception("QuintoAndar: erro ao parsear item")
            return None

    async def scrape(self) -> AsyncGenerator[list[Listing], None]:
        """Override base scrape() to use the JSON API with offset pagination."""
        seen_ids: set[str] = set()
        known_ids: frozenset[str] = self.job.known_ids

        for page in range(self.job.max_pages):
            offset = page * PAGE_SIZE
            payload = self._build_payload(offset)

            try:
                scraperapi_key = os.getenv("SCRAPERAPI_KEY")
                if scraperapi_key:
                    proxy_url = f"http://api.scraperapi.com/?api_key={scraperapi_key}&url={urllib.parse.quote_plus(API_URL)}"
                    response = await self.client.post(
                        proxy_url,
                        json=payload,
                        headers={"Content-Type": "application/json", "Accept": "application/json"},
                    )
                else:
                    response = await self.client.post(
                        API_URL,
                        json=payload,
                        headers={"Content-Type": "application/json", "Accept": "application/json"},
                    )
                response.raise_for_status()
                data = response.json()
            except Exception:
                logger.exception("QuintoAndar: erro na requisição API (offset=%d)", offset)
                break

            raw_hits = data.get("hits", {})
            if isinstance(raw_hits, dict):
                houses = raw_hits.get("hits", [])
            elif isinstance(raw_hits, list):
                houses = raw_hits
            else:
                houses = []

            if not houses:
                logger.info("QuintoAndar: sem resultados no offset %d", offset)
                break

            # Parse all items, deduplicating within this session
            session_new: list[Listing] = []
            for house in houses:
                listing = self._parse_item(house)
                if listing is None or listing.external_id in seen_ids:
                    continue
                seen_ids.add(listing.external_id)
                session_new.append(listing)

            if not session_new:
                # Every item on this page was already seen this session → we looped, stop
                break

            # Filter out listings already in the database — yield only truly new ones
            new_listings = [l for l in session_new if l.external_id not in known_ids]

            if new_listings:
                logger.info("QuintoAndar: página %d → %d novos, %d já existentes ignorados",
                            page + 1, len(new_listings), len(session_new) - len(new_listings))
                yield new_listings
            else:
                logger.info("QuintoAndar: página %d → todos os %d anúncios já existem no banco, continuando...",
                            page + 1, len(session_new))

            # Stop if the API returned fewer items than the page size (last page)
            if len(houses) < PAGE_SIZE:
                break
