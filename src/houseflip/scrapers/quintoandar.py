"""QuintoAndar scraper — parses schema.org JSON-LD listings embedded in SSR HTML."""

import json
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from parsel import Selector

from houseflip.models.listing import Listing, ListingSource, ListingType, PropertyType
from houseflip.scrapers.base import BaseScraper, slugify

logger = logging.getLogger(__name__)

BASE_URL = "https://www.quintoandar.com.br"

_LISTING_TYPE_SLUG = {
    ListingType.SALE: "comprar",
    ListingType.RENT: "alugar",
}

_PROP_TYPE_PARAM = {
    PropertyType.APARTMENT: "Apartamento",
    PropertyType.HOUSE: "Casa",
    PropertyType.LOT: "Terreno",
    PropertyType.COMMERCIAL: "Comercial",
}

_SCHEMA_TYPE_TO_PROP = {
    "Apartment": PropertyType.APARTMENT,
    "House": PropertyType.HOUSE,
    "SingleFamilyResidence": PropertyType.HOUSE,
    "LandmarksOrHistoricalBuildings": PropertyType.COMMERCIAL,
}

# Minimum listings to assume there's a next page
_PAGE_SIZE = 12


class QuintoAndarScraper(BaseScraper):
    source = ListingSource.QUINTOANDAR

    def _build_url(self, page: int) -> str:
        listing_slug = _LISTING_TYPE_SLUG[self.job.listing_type]
        city_slug = slugify(self.job.city)
        state = self.job.state.lower()
        prop_param = _PROP_TYPE_PARAM.get(self.job.property_type, "Apartamento")

        location = f"{city_slug}-{state}-brasil"
        url = f"{BASE_URL}/{listing_slug}/imovel/{location}/"

        params = [f"tipoImovel={prop_param}"]
        if page > 1:
            params.append(f"pagina={page}")

        return f"{url}?{'&'.join(params)}"

    def _parse_listings(self, html: str) -> list[Listing]:
        sel = Selector(text=html)
        json_ld_blocks = sel.css('script[type="application/ld+json"]::text').getall()

        neigh_filter = slugify(self.job.neighborhood) if self.job.neighborhood else None

        results = []
        for block in json_ld_blocks:
            try:
                data = json.loads(block)
            except json.JSONDecodeError:
                continue

            schema_type = data.get("@type", "")
            if schema_type not in _SCHEMA_TYPE_TO_PROP:
                continue

            listing = self._parse_item(data)
            if listing:
                if neigh_filter and neigh_filter not in slugify(listing.neighborhood):
                    continue
                results.append(listing)

        if not results:
            logger.warning("QuintoAndar: nenhum anúncio encontrado na página")

        return results

    def _parse_item(self, data: dict) -> Listing | None:
        try:
            url = data.get("url", "")
            if not url:
                return None

            # external_id from URL: /imovel/895287325/comprar/...
            id_match = re.search(r"/imovel/(\d+)/", url)
            if not id_match:
                return None
            external_id = id_match.group(1)

            # Price from potentialAction
            action = data.get("potentialAction", {})
            price_raw = action.get("price", 0)
            try:
                price = Decimal(str(price_raw))
            except InvalidOperation:
                return None
            if price <= 0:
                return None

            # Area
            area_raw = data.get("floorSize")
            area = None
            if area_raw is not None:
                try:
                    area = Decimal(str(area_raw))
                except InvalidOperation:
                    pass

            # Rooms
            bedrooms = data.get("numberOfBedrooms") or data.get("numberOfRooms")
            bathrooms = data.get("numberOfFullBathrooms") or data.get("numberOfBathroomsTotal")
            try:
                bedrooms = int(bedrooms) if bedrooms is not None else None
                bathrooms = int(bathrooms) if bathrooms is not None else None
            except (ValueError, TypeError):
                bedrooms = None
                bathrooms = None

            # Address — "Rua X, Bairro, Cidade" or "Bairro, Cidade"
            address_raw = data.get("address", "")
            neighborhood, city, street = self._parse_address(address_raw)

            # Property type from @type
            schema_type = data.get("@type", "")
            prop_type = _SCHEMA_TYPE_TO_PROP.get(schema_type, self.job.property_type)

            # Listing type from URL path or action type
            action_type = action.get("@type", "")
            if "Rent" in action_type or "/alugar/" in url:
                listing_type = ListingType.RENT
            else:
                listing_type = ListingType.SALE

            # Image
            image = data.get("image", "")
            images = [image] if image else []

            return Listing(
                external_id=external_id,
                source=ListingSource.QUINTOANDAR,
                url=url,
                listing_type=listing_type,
                property_type=prop_type,
                city=city,
                neighborhood=neighborhood,
                street=street or None,
                price_brl=price,
                area_m2=area,
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                parking_spots=None,
                title=data.get("name"),
                description=data.get("description"),
                images=images,
                scraped_at=datetime.now(timezone.utc),
            )
        except Exception:
            logger.exception("QuintoAndar: erro ao parsear item")
            return None

    def _parse_address(self, address: str) -> tuple[str, str, str | None]:
        """Parse 'Rua X, Bairro, Cidade' → (neighborhood, city, street)."""
        parts = [p.strip() for p in address.split(",")]
        if len(parts) >= 3:
            return parts[-2], parts[-1], ", ".join(parts[:-2])
        if len(parts) == 2:
            return parts[0], parts[1], None
        return address, self.job.city, None

    def _has_next_page(self, html: str, page: int) -> bool:
        sel = Selector(text=html)
        json_ld_blocks = sel.css('script[type="application/ld+json"]::text').getall()
        count = sum(
            1 for block in json_ld_blocks
            if '"@type"' in block and any(t in block for t in _SCHEMA_TYPE_TO_PROP)
        )
        return count >= _PAGE_SIZE
