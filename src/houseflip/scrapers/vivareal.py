"""VivaReal scraper — parses HTML listings (SSR, same structure as ZapImóveis)."""

import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from parsel import Selector

from houseflip.models.listing import Listing, ListingSource, ListingType, PropertyType
from houseflip.models.scrape_config import ScrapeJob
from houseflip.scrapers.base import BaseScraper, slugify

logger = logging.getLogger(__name__)

BASE_URL = "https://www.vivareal.com.br"

_PROP_TYPE_SLUG = {
    PropertyType.APARTMENT: "apartamento_residencial",
    PropertyType.HOUSE: "casa_residencial",
    PropertyType.LOT: "terreno_residencial",
    PropertyType.COMMERCIAL: "imovel-comercial",
}


class VivaRealScraper(BaseScraper):
    source = ListingSource.VIVAREAL

    def __init__(self, job: ScrapeJob) -> None:
        super().__init__(job)

    def _build_url(self, page: int) -> str:
        city_slug = slugify(self.job.city)
        state = self.job.state.lower()
        listing_type = self.job.listing_type.value
        prop_slug = _PROP_TYPE_SLUG.get(self.job.property_type, "apartamento_residencial")

        path = f"/{listing_type}/{state}/{city_slug}/{prop_slug}/"
        url = f"{BASE_URL}{path}?pagina={page}"
        if self.job.neighborhood:
            url += f"&bairros={slugify(self.job.neighborhood)}"
        return url

    def _parse_listings(self, html: str) -> list[Listing]:
        sel = Selector(text=html)
        items = sel.css('li[data-cy="rp-property-cd"]')
        if not items:
            logger.warning("VivaReal: nenhum anúncio encontrado na página")
            return []

        neigh_filter = slugify(self.job.neighborhood) if self.job.neighborhood else None

        results = []
        for item in items:
            listing = self._parse_item(item)
            if listing:
                if neigh_filter and neigh_filter not in slugify(listing.neighborhood):
                    continue
                results.append(listing)
        return results

    def _parse_item(self, item: Selector) -> Listing | None:
        try:
            href = item.css("a::attr(href)").get("")
            if not href:
                return None
            if not href.startswith("http"):
                href = BASE_URL + href

            id_match = re.search(r"id-(\d+)", href)
            if not id_match:
                return None
            external_id = id_match.group(1)

            # Price
            price_texts = item.css('[data-cy="rp-cardProperty-price-txt"] p::text').getall()
            price_text = next((t for t in price_texts if "R$" in t), "")
            price_clean = re.sub(r"[^\d]", "", price_text.replace(".", ""))
            try:
                price = Decimal(price_clean)
            except InvalidOperation:
                return None
            if price <= 0:
                return None

            # Area
            area_text = (item.css('[data-cy="rp-cardProperty-propertyArea-txt"] h3::text').get() or "").strip()
            area_match = re.search(r"[\d]+", area_text.replace(".", ""))
            area = Decimal(area_match.group()) if area_match else None

            # Bedrooms / bathrooms / parking
            def _int_field(cy: str) -> int | None:
                t = (item.css(f'[data-cy="{cy}"] h3::text').get() or "").strip()
                return int(t) if t.isdigit() else None

            bedrooms = _int_field("rp-cardProperty-bedroomQuantity-txt")
            bathrooms = _int_field("rp-cardProperty-bathroomQuantity-txt")
            parking = _int_field("rp-cardProperty-parkingSpacesQuantity-txt")

            # Location
            loc_text = (item.css('[data-cy="rp-cardProperty-location-txt"]::text').get() or "").strip()
            parts = [p.strip() for p in loc_text.split(",")]
            neighborhood = parts[0] if parts else ""
            city = parts[1] if len(parts) > 1 else self.job.city

            street = (item.css('[data-cy="rp-cardProperty-street-txt"]::text').get() or "").strip() or None

            title = item.css("a::attr(title)").get()

            listing_type = ListingType.RENT if "/aluguel/" in href else ListingType.SALE

            return Listing(
                external_id=external_id,
                source=ListingSource.VIVAREAL,
                url=href,
                listing_type=listing_type,
                property_type=self.job.property_type,
                city=city,
                neighborhood=neighborhood,
                street=street,
                price_brl=price,
                area_m2=area,
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                parking_spots=parking,
                title=title,
                scraped_at=datetime.now(timezone.utc),
            )
        except Exception:
            logger.exception("VivaReal: erro ao parsear item")
            return None

    def _has_next_page(self, html: str, page: int) -> bool:
        sel = Selector(text=html)
        items = sel.css('li[data-cy="rp-property-cd"]')
        return len(items) >= 30
