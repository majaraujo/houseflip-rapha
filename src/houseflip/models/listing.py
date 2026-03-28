from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, HttpUrl, computed_field


class ListingSource(StrEnum):
    ZAPIMOVEIS = "zapimoveis"
    VIVAREAL = "vivareal"
    OLX = "olx"
    CHAVESNAMAO = "chavesnamao"
    QUINTOANDAR = "quintoandar"


class ListingType(StrEnum):
    SALE = "venda"
    RENT = "aluguel"


class PropertyType(StrEnum):
    APARTMENT = "apartamento"
    HOUSE = "casa"
    LOT = "terreno"
    COMMERCIAL = "comercial"


class Listing(BaseModel):
    model_config = {"frozen": True}

    external_id: str
    source: ListingSource
    url: str
    listing_type: ListingType
    property_type: PropertyType
    city: str
    neighborhood: str
    street: str | None = None
    price_brl: Decimal
    area_m2: Decimal | None = None
    bedrooms: int | None = None
    bathrooms: int | None = None
    parking_spots: int | None = None
    title: str | None = None
    description: str | None = None
    images: list[str] = []
    scraped_at: datetime

    @computed_field
    @property
    def price_per_m2(self) -> Decimal | None:
        if self.area_m2 and self.area_m2 > 0:
            return (self.price_brl / self.area_m2).quantize(Decimal("0.01"))
        return None
