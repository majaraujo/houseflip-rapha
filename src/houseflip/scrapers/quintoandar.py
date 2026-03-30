"""QuintoAndar scraper — uses internal JSON search API (POST)."""

import asyncio
import logging
import random
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from houseflip.models.listing import Listing, ListingSource, ListingType, PropertyType
from houseflip.models.scrape_config import ScrapeJob
from houseflip.scrapers.base import BaseScraper, slugify

logger = logging.getLogger(__name__)

API_URL = "https://apigw.prod.quintoandar.com.br/house-listing-search/v2/search/list"
PAGE_SIZE = 20

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

# City-level bounding boxes
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

# Neighbourhood-level bounding boxes for São Paulo (tighter viewport = fewer irrelevant results)
_NEIGHBORHOOD_COORDS: dict[str, dict] = {
    "moema": {
        "lat": -23.602021, "lng": -46.672103,
        "viewport": {"north": -23.56, "south": -23.65, "east": -46.63, "west": -46.70},
    },
    "pinheiros": {
        "lat": -23.5643, "lng": -46.6836,
        "viewport": {"north": -23.54, "south": -23.59, "east": -46.66, "west": -46.71},
    },
    "vila-mariana": {
        "lat": -23.5874, "lng": -46.6353,
        "viewport": {"north": -23.57, "south": -23.61, "east": -46.61, "west": -46.66},
    },
    "itaim-bibi": {
        "lat": -23.5850, "lng": -46.6769,
        "viewport": {"north": -23.56, "south": -23.61, "east": -46.65, "west": -46.71},
    },
    "brooklin": {
        "lat": -23.6199, "lng": -46.6960,
        "viewport": {"north": -23.60, "south": -23.64, "east": -46.67, "west": -46.72},
    },
    "perdizes": {
        "lat": -23.5367, "lng": -46.6642,
        "viewport": {"north": -23.52, "south": -23.56, "east": -46.64, "west": -46.69},
    },
    "jardins": {
        "lat": -23.5658, "lng": -46.6566,
        "viewport": {"north": -23.55, "south": -23.58, "east": -46.64, "west": -46.68},
    },
    "campo-belo": {
        "lat": -23.6218, "lng": -46.6643,
        "viewport": {"north": -23.60, "south": -23.64, "east": -46.64, "west": -46.69},
    },
    "santo-andre": {
        "lat": -23.6639, "lng": -46.5338,
        "viewport": {"north": -23.64, "south": -23.69, "east": -46.51, "west": -46.56},
    },
    "vila-olimpia": {
        "lat": -23.5960, "lng": -46.6853,
        "viewport": {"north": -23.58, "south": -23.61, "east": -46.67, "west": -46.70},
    },
}


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

    def _get_coords(self) -> dict:
        """Return the tightest available viewport: neighbourhood > city > default."""
        if self.job.neighborhood:
            neigh_slug = slugify(self.job.neighborhood)
            if neigh_slug in _NEIGHBORHOOD_COORDS:
                return _NEIGHBORHOOD_COORDS[neigh_slug]
        city_slug = slugify(self.job.city)
        return _CITY_COORDS.get(city_slug, _DEFAULT_COORDS)

    def _build_payload(self, offset: int) -> dict:
        city_slug = slugify(self.job.city)
        state = self.job.state.lower()
        coords = self._get_coords()

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
                "installations", "amenities", "location",
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

            # Build a descriptive title from available fields
            type_label = "Casa" if prop_type == PropertyType.HOUSE else "Apartamento"
            action = "para alugar" if listing_type == ListingType.RENT else "para comprar"
            parts = [f"{type_label} {action}"]
            if area:
                parts.append(f"com {int(area)} m²")
            if bedrooms:
                parts.append(f"{bedrooms} quarto{'s' if bedrooms > 1 else ''}")
            if bathrooms:
                parts.append(f"{bathrooms} banheiro{'s' if bathrooms > 1 else ''}")
            if parking:
                parts.append(f"{parking} vaga{'s' if parking > 1 else ''}")
            location_parts = [p for p in [address, neighborhood, city] if p]
            title = ", ".join(parts)
            if location_parts:
                title += " em " + ", ".join(location_parts)

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
                title=title,
                scraped_at=datetime.now(timezone.utc),
            )
        except Exception:
            logger.exception("QuintoAndar: erro ao parsear item")
            return None

    async def scrape(self) -> AsyncGenerator[list[Listing], None]:
        """Override base scrape() to use the JSON API with offset pagination."""
        seen_ids: set[str] = set()
        total_available: int | None = None

        # Neighbourhood slug for post-filtering (API filters by viewport, not by name)
        neigh_slug = slugify(self.job.neighborhood) if self.job.neighborhood else None

        for page in range(self.job.max_pages):
            offset = page * PAGE_SIZE
            payload = self._build_payload(offset)

            try:
                response = await self.client.post(
                    API_URL,
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "Referer": "https://www.quintoandar.com.br/",
                    },
                )
                response.raise_for_status()
                data = response.json()
            except Exception:
                logger.exception("QuintoAndar: erro na requisição API (offset=%d)", offset)
                break

            raw_hits = data.get("hits", {})
            if isinstance(raw_hits, dict):
                # Capture total count on first page
                if total_available is None:
                    total_info = raw_hits.get("total", {})
                    if isinstance(total_info, dict):
                        total_available = total_info.get("value")
                        logger.info("QuintoAndar: total disponível na API = %d", total_available or 0)
                houses = raw_hits.get("hits", [])
            elif isinstance(raw_hits, list):
                houses = raw_hits
            else:
                houses = []

            if not houses:
                logger.info("QuintoAndar: sem resultados no offset %d", offset)
                break

            new_listings = []
            for house in houses:
                listing = self._parse_item(house)
                if listing is None or listing.external_id in seen_ids:
                    continue
                # Post-filter by neighbourhood: the API uses viewport (bounding box),
                # so results may include nearby neighbourhoods — keep only the target.
                if neigh_slug and listing.neighborhood:
                    if slugify(listing.neighborhood) != neigh_slug:
                        continue
                seen_ids.add(listing.external_id)
                new_listings.append(listing)

            if not new_listings:
                break

            yield new_listings

            # Stop when we've collected everything the API has
            if total_available and len(seen_ids) >= total_available:
                logger.info("QuintoAndar: todos os %d anúncios coletados", total_available)
                break

            if len(houses) < PAGE_SIZE:
                break

            # Delay entre páginas para evitar rate limiting
            await asyncio.sleep(self.job.request_delay_seconds + random.uniform(-0.3, 0.5))
