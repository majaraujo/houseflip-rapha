from houseflip.scrapers.base import BaseScraper
from houseflip.scrapers.zapimoveis import ZapImoveisScraper
from houseflip.scrapers.vivareal import VivaRealScraper
from houseflip.scrapers.olx import OlxScraper
from houseflip.scrapers.chavesnamao import ChavesNaMaoScraper
from houseflip.scrapers.quintoandar import QuintoAndarScraper
from houseflip.models.listing import ListingSource

SCRAPER_REGISTRY: dict[ListingSource, type[BaseScraper]] = {
    ListingSource.ZAPIMOVEIS: ZapImoveisScraper,
    ListingSource.VIVAREAL: VivaRealScraper,
    ListingSource.OLX: OlxScraper,
    ListingSource.CHAVESNAMAO: ChavesNaMaoScraper,
    ListingSource.QUINTOANDAR: QuintoAndarScraper,
}

__all__ = [
    "BaseScraper", "ZapImoveisScraper", "VivaRealScraper",
    "OlxScraper", "ChavesNaMaoScraper", "QuintoAndarScraper", "SCRAPER_REGISTRY",
]
