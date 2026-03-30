"""OLX scraper — parses __NEXT_DATA__ JSON from Next.js pages."""

import json
import logging
import re
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from parsel import Selector

from houseflip.models.listing import Listing, ListingSource, ListingType, PropertyType
from houseflip.models.scrape_config import ScrapeJob
from houseflip.scrapers.base import BaseScraper, slugify

logger = logging.getLogger(__name__)

BASE_URL = "https://www.olx.com.br"

_PROP_TYPE_SLUG = {
    PropertyType.APARTMENT: "apartamentos",
    PropertyType.HOUSE: "casas",
    PropertyType.LOT: "terrenos",
    PropertyType.COMMERCIAL: "imoveis-comerciais",
}

_CATEGORY_TO_PROP_TYPE = {
    "apartamentos": PropertyType.APARTMENT,
    "casas": PropertyType.HOUSE,
    "terrenos": PropertyType.LOT,
    "comercial": PropertyType.COMMERCIAL,
}


class OlxScraper(BaseScraper):
    source = ListingSource.OLX

    def _build_url(self, page: int) -> str:
        city_slug = slugify(self.job.city)
        state = self.job.state.lower()
        listing_type = self.job.listing_type.value
        prop_slug = _PROP_TYPE_SLUG.get(self.job.property_type, "apartamentos")

        region = f"{city_slug}-e-regiao"
        path = f"/imoveis/{prop_slug}/{listing_type}/estado-{state}/{region}"

        params: dict[str, str] = {}
        if page > 1:
            params["o"] = str(page)
        if self.job.neighborhood:
            params["q"] = self.job.neighborhood

        return f"{BASE_URL}{path}?{urllib.parse.urlencode(params)}" if params else f"{BASE_URL}{path}"

    def _parse_listings(self, html: str) -> list[Listing]:
        sel = Selector(text=html)
        raw_json = sel.css("script#__NEXT_DATA__::text").get()
        if not raw_json:
            logger.warning("OLX: __NEXT_DATA__ não encontrado")
            return []

        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            logger.exception("OLX: falha ao parsear __NEXT_DATA__")
            return []

        try:
            ads = data["props"]["pageProps"]["ads"]
        except (KeyError, TypeError):
            logger.warning("OLX: estrutura inesperada no __NEXT_DATA__")
            return []

        neigh_filter = slugify(self.job.neighborhood) if self.job.neighborhood else None

        results = []
        for ad in ads:
            listing = self._parse_item(ad)
            if listing:
                if neigh_filter and slugify(listing.neighborhood) != neigh_filter:
                    continue
                results.append(listing)
        return results

    def _parse_item(self, ad: dict) -> Listing | None:
        try:
            external_id = str(ad.get("listId", ""))
            if not external_id:
                return None

            url = ad.get("url") or ad.get("friendlyUrl", "")
            if not url:
                return None

            # Price — "R$ 220.000" or "R$220.000"
            price_raw = ad.get("priceValue") or ad.get("price", "")
            price_clean = re.sub(r"[^\d]", "", str(price_raw).replace(".", ""))
            try:
                price = Decimal(price_clean)
            except InvalidOperation:
                return None
            if price <= 0:
                return None

            # Properties dict {name: value}
            props = {p["name"]: p["value"] for p in ad.get("properties", []) if "name" in p and "value" in p}

            # Area — "42m²" → 42
            size_raw = props.get("size", "")
            area_match = re.search(r"[\d]+", str(size_raw).replace(".", ""))
            area = Decimal(area_match.group()) if area_match else None

            # Rooms / bathrooms / garage
            def _int_prop(key: str) -> int | None:
                val = props.get(key)
                try:
                    return int(val) if val is not None else None
                except (ValueError, TypeError):
                    return None

            bedrooms = _int_prop("rooms")
            bathrooms = _int_prop("bathrooms")
            parking = _int_prop("garage_spaces")

            # Location
            loc = ad.get("locationDetails", {})
            neighborhood = loc.get("neighbourhood") or ad.get("location", "").split(",")[-1].strip()
            city = loc.get("municipality") or self.job.city

            # Listing type from real_estate_type property
            rt = props.get("real_estate_type", "").lower()
            listing_type = ListingType.RENT if "aluguel" in rt else ListingType.SALE

            # Property type from category
            cat = ad.get("category", "").lower()
            prop_type = _CATEGORY_TO_PROP_TYPE.get(cat, self.job.property_type)

            images = [
                img["original"]
                for img in ad.get("images", [])[:5]
                if img.get("original")
            ]

            return Listing(
                external_id=external_id,
                source=ListingSource.OLX,
                url=url,
                listing_type=listing_type,
                property_type=prop_type,
                city=city,
                neighborhood=neighborhood,
                price_brl=price,
                area_m2=area,
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                parking_spots=parking,
                title=ad.get("title") or ad.get("subject"),
                images=images,
                scraped_at=datetime.now(timezone.utc),
            )
        except Exception:
            logger.exception("OLX: erro ao parsear item")
            return None

    def _has_next_page(self, html: str, page: int) -> bool:
        sel = Selector(text=html)
        raw_json = sel.css("script#__NEXT_DATA__::text").get()
        if not raw_json:
            return False
        try:
            data = json.loads(raw_json)
            pp = data["props"]["pageProps"]
            total = pp.get("totalOfAds", 0)
            page_size = pp.get("pageSize", 50)
            return page < (total // page_size)
        except (KeyError, TypeError):
            return False
