"""Price deviation analysis using Polars.

Computes a composite opportunity score per listing relative to its neighborhood.
Higher score = better deal (listing priced below neighborhood average).
"""

import logging
from datetime import datetime

import polars as pl

from houseflip.storage.database import Database

logger = logging.getLogger(__name__)

# Weight for price_per_m2 vs absolute price in the composite score
# price_per_m2 is weighted more because it normalizes for size differences
_W_PRICE = 0.35
_W_PRICE_M2 = 0.65
_MIN_LISTINGS_PER_NEIGHBORHOOD = 3


class PriceDeviationService:
    def __init__(self, db: Database) -> None:
        self._db = db

    def _load_listings(
        self,
        city: str | None = None,
        listing_type: str | None = None,
        property_type: str | None = None,
        scraped_after: datetime | None = None,
        sources: list[str] | None = None,
    ) -> pl.DataFrame:
        conditions = ["is_active = TRUE", "price_brl > 0"]
        params: list = []

        if city:
            conditions.append("lower(city) = lower(?)")
            params.append(city)
        if listing_type:
            conditions.append("listing_type = ?")
            params.append(listing_type)
        if property_type:
            conditions.append("property_type = ?")
            params.append(property_type)
        if scraped_after:
            conditions.append("scraped_at >= ?")
            params.append(scraped_after)
        if sources:
            placeholders = ", ".join("?" * len(sources))
            conditions.append(f"source IN ({placeholders})")
            params.extend(sources)

        where = " AND ".join(conditions)
        result = self._db.execute(
            f"""
            SELECT id, external_id, source, url, listing_type, property_type,
                   city, neighborhood, price_brl, area_m2, price_per_m2,
                   bedrooms, bathrooms, parking_spots, title
              FROM listings
             WHERE {where}
            """,
            params if params else None,
        )
        rows = result.fetchall()
        cols = ["id", "external_id", "source", "url", "listing_type", "property_type",
                "city", "neighborhood", "price_brl", "area_m2", "price_per_m2",
                "bedrooms", "bathrooms", "parking_spots", "title"]

        if not rows:
            return pl.DataFrame(schema={c: pl.Utf8 for c in cols})

        return pl.DataFrame(rows, schema=cols, orient="row")

    def compute_opportunities(
        self,
        city: str | None = None,
        listing_type: str | None = None,
        property_type: str | None = None,
        min_listings: int = _MIN_LISTINGS_PER_NEIGHBORHOOD,
        scraped_after: datetime | None = None,
        neighborhoods: list[str] | None = None,
        area_min: float | None = None,
        area_max: float | None = None,
        sources: list[str] | None = None,
    ) -> pl.DataFrame:
        """Return listings enriched with opportunity scores, sorted best first."""
        df = self._load_listings(city, listing_type, property_type, scraped_after, sources)
        if df.is_empty():
            return df

        # Cast numeric columns
        df = df.with_columns([
            pl.col("price_brl").cast(pl.Float64),
            pl.col("area_m2").cast(pl.Float64),
            pl.col("price_per_m2").cast(pl.Float64),
        ])

        # Compute neighborhood stats
        stats = (
            df.group_by(["city", "neighborhood", "listing_type", "property_type"])
            .agg([
                pl.count("id").alias("n"),
                pl.col("price_brl").mean().alias("mean_price"),
                pl.col("price_brl").std().alias("std_price"),
                pl.col("price_brl").median().alias("median_price"),
                pl.col("price_per_m2").mean().alias("mean_price_m2"),
                pl.col("price_per_m2").std().alias("std_price_m2"),
                pl.col("price_per_m2").median().alias("median_price_m2"),
            ])
            .filter(pl.col("n") >= min_listings)
        )

        # Join listings with their neighborhood stats
        enriched = df.join(
            stats,
            on=["city", "neighborhood", "listing_type", "property_type"],
            how="inner",
        )

        # Compute z-scores (std=0 means all listings have same price → z=0)
        enriched = enriched.with_columns([
            pl.when(pl.col("std_price") > 0)
            .then((pl.col("price_brl") - pl.col("mean_price")) / pl.col("std_price"))
            .otherwise(pl.lit(0.0))
            .alias("z_price"),
            pl.when(pl.col("std_price_m2") > 0)
            .then((pl.col("price_per_m2") - pl.col("mean_price_m2")) / pl.col("std_price_m2"))
            .otherwise(pl.lit(0.0))
            .alias("z_price_m2"),
        ])

        # Composite opportunity score: negate z-score so lower price = higher score
        enriched = enriched.with_columns([
            pl.when(pl.col("z_price_m2").is_not_null())
            .then(
                -1 * (_W_PRICE * pl.col("z_price") + _W_PRICE_M2 * pl.col("z_price_m2"))
            )
            .otherwise(-1 * pl.col("z_price"))
            .alias("opportunity_score"),
        ])

        # Percentage below/above neighborhood median
        enriched = enriched.with_columns([
            ((pl.col("price_brl") - pl.col("median_price")) / pl.col("median_price") * 100)
            .alias("pct_vs_median"),
        ])

        result = (
            enriched.sort("opportunity_score", descending=True)
            .select([
                "id", "external_id", "source", "url",
                "city", "neighborhood", "listing_type", "property_type",
                "price_brl", "area_m2", "price_per_m2",
                "bedrooms", "bathrooms", "parking_spots", "title",
                "z_price", "z_price_m2", "opportunity_score",
                "median_price", "median_price_m2", "pct_vs_median",
                "n",
            ])
        )

        # Post-compute filters (applied after z-score so neighborhood stats stay intact)
        if neighborhoods:
            result = result.filter(pl.col("neighborhood").is_in(neighborhoods))
        if area_min is not None:
            result = result.filter(pl.col("area_m2").cast(pl.Float64) >= area_min)
        if area_max is not None:
            result = result.filter(pl.col("area_m2").cast(pl.Float64) <= area_max)

        return result

    def neighborhood_summary(
        self,
        city: str | None = None,
        listing_type: str | None = None,
        property_type: str | None = None,
        min_listings: int = _MIN_LISTINGS_PER_NEIGHBORHOOD,
        scraped_after: datetime | None = None,
        neighborhoods: list[str] | None = None,
        area_min: float | None = None,
        area_max: float | None = None,
        sources: list[str] | None = None,
    ) -> pl.DataFrame:
        """Return one row per neighborhood with aggregate stats and best opportunity score."""
        opp = self.compute_opportunities(
            city, listing_type, property_type, min_listings,
            scraped_after=scraped_after, neighborhoods=neighborhoods,
            area_min=area_min, area_max=area_max, sources=sources,
        )
        if opp.is_empty():
            return opp

        return (
            opp.group_by(["city", "neighborhood", "listing_type", "property_type"])
            .agg([
                pl.col("n").first().alias("listing_count"),
                pl.col("median_price").first(),
                pl.col("median_price_m2").first(),
                pl.col("opportunity_score").max().alias("best_opportunity_score"),
                pl.col("opportunity_score").mean().alias("avg_opportunity_score"),
                pl.col("pct_vs_median").min().alias("best_pct_vs_median"),
            ])
            .sort("best_opportunity_score", descending=True)
        )
