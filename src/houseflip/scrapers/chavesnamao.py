"""Chaves na Mão scraper — parses SSR HTML listing cards.

The site uses Next.js App Router with SSR — listing data is in the HTML cards
and in JSON-LD. Subsequent pages require JS hydration so only page 1 is reliable.
"""

import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from parsel import Selector

from houseflip.models.listing import Listing, ListingSource, ListingType, PropertyType
from houseflip.models.scrape_config import ScrapeJob
from houseflip.scrapers.base import BaseScraper, slugify

logger = logging.getLogger(__name__)

BASE_URL = "https://www.chavesnamao.com.br"

_PROP_TYPE_SLUG = {
    PropertyType.APARTMENT: "apartamentos",
    PropertyType.HOUSE: "casas",
    PropertyType.LOT: "terrenos",
    PropertyType.COMMERCIAL: "imoveis-comerciais",
}

_LISTING_TYPE_SLUG = {
    ListingType.SALE: "a-venda",
    ListingType.RENT: "para-alugar",
}


class ChavesNaMaoScraper(BaseScraper):
    source = ListingSource.CHAVESNAMAO

    def _build_url(self, page: int) -> str:
        prop_slug = _PROP_TYPE_SLUG.get(self.job.property_type, "apartamentos")
        listing_slug = _LISTING_TYPE_SLUG.get(self.job.listing_type, "a-venda")
        state = self.job.state.lower()
        city_slug = slugify(self.job.city)
        return f"{BASE_URL}/{prop_slug}-{listing_slug}/{state}-{city_slug}/"

    def _parse_listings(self, html: str) -> list[Listing]:
        sel = Selector(text=html)
        cards = sel.css('[id^="rc-"]')
        if not cards:
            logger.warning("Chaves na Mão: nenhum card encontrado")
            return []

        results = []
        for card in cards:
            listing = self._parse_card(card)
            if listing:
                results.append(listing)
        return results

    def _parse_card(self, card: Selector) -> Listing | None:
        try:
            card_id = card.attrib.get("id", "")
            external_id = card_id.replace("rc-", "").strip()
            if not external_id:
                return None

            href = card.css("a::attr(href)").get("").strip()
            if not href:
                return None
            url = BASE_URL + href if href.startswith("/") else href

            title = card.css("a::attr(title)").get() or card.css("h2::text").get()

            # Address — two <p> inside <address>: street and "Neighborhood, City/State"
            addr_ps = card.css("address p::attr(title)").getall()
            street = addr_ps[0].strip() if addr_ps else None
            if street and street.lower() in ("endereço indisponível", "endere\u00e7o indispon\u00edvel"):
                street = None

            neighborhood, city = "", self.job.city
            if len(addr_ps) > 1:
                loc = addr_ps[1].strip()
                parts = [p.strip() for p in loc.split(",")]
                neighborhood = parts[0]
                if len(parts) > 1:
                    city = parts[1].split("/")[0].strip()

            # Features — <p title="62 Área útil">, <p title="3 Quartos">, etc.
            def _feature(keyword: str) -> int | None:
                titles = card.css(f'[title*="{keyword}"]::attr(title)').getall()
                for t in titles:
                    m = re.search(r"\d+", t)
                    if m:
                        return int(m.group())
                return None

            area_val = _feature("Área")
            area = Decimal(area_val) if area_val else None
            bedrooms = _feature("Quarto")
            bathrooms = _feature("Banheiro")
            parking = _feature("Garagem") or _feature("Vaga")

            # Price — <p aria-label="Preço"><b>R$ 600.000</b>
            price_text = card.css('[aria-label="Preço"] b::text').get() or ""
            price_clean = re.sub(r"[^\d]", "", price_text.replace(".", ""))
            try:
                price = Decimal(price_clean)
            except InvalidOperation:
                return None
            if price <= 0:
                return None

            listing_type = (
                ListingType.RENT if "para-alugar" in url else ListingType.SALE
            )

            return Listing(
                external_id=external_id,
                source=ListingSource.CHAVESNAMAO,
                url=url,
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
            logger.exception("Chaves na Mão: erro ao parsear card")
            return None

    def _has_next_page(self, html: str, page: int) -> bool:
        # The site's SSR only delivers the first page of results.
        # Subsequent pages require JS hydration and return identical content.
        return False
