"""Persistence operations for Listing and ScrapeRun records."""

import uuid
from datetime import datetime, timezone

from houseflip.models.listing import Listing
from houseflip.storage.database import Database


class ListingRepository:
    def __init__(self, db: Database) -> None:
        self._db = db

    # ------------------------------------------------------------------
    # Scrape run lifecycle
    # ------------------------------------------------------------------

    def create_scrape_run(
        self,
        source: str,
        city: str,
        neighborhood: str | None,
        listing_type: str,
        property_type: str,
    ) -> str:
        run_id = str(uuid.uuid4())
        self._db.execute(
            """
            INSERT INTO scrape_runs
                (id, source, city, neighborhood, listing_type, property_type, started_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'running')
            """,
            [run_id, source, city, neighborhood, listing_type, property_type,
             datetime.now(timezone.utc)],
        )
        return run_id

    def finish_scrape_run(self, run_id: str, total_found: int, status: str = "done") -> None:
        self._db.execute(
            """
            UPDATE scrape_runs
               SET finished_at = ?, total_found = ?, status = ?
             WHERE id = ?
            """,
            [datetime.now(timezone.utc), total_found, status, run_id],
        )

    def list_scrape_runs(self, limit: int = 20) -> list[dict]:
        rows = self._db.execute(
            """
            SELECT id, source, city, neighborhood, listing_type, property_type,
                   started_at, finished_at, total_found, status
              FROM scrape_runs
             ORDER BY started_at DESC
             LIMIT ?
            """,
            [limit],
        ).fetchall()
        cols = ["id", "source", "city", "neighborhood", "listing_type", "property_type",
                "started_at", "finished_at", "total_found", "status"]
        return [dict(zip(cols, row)) for row in rows]

    # ------------------------------------------------------------------
    # Listings
    # ------------------------------------------------------------------

    def upsert_listings(
        self,
        listings: list[Listing],
        scrape_run_id: str,
        city_override: str | None = None,
    ) -> int:
        """Insert or update listings. Returns count of new/updated rows."""
        rows = [
            [
                str(uuid.uuid4()),
                listing.external_id,
                listing.source,
                listing.url,
                listing.listing_type,
                listing.property_type,
                city_override if city_override else listing.city,
                listing.neighborhood,
                listing.street,
                float(listing.price_brl),
                float(listing.area_m2) if listing.area_m2 else None,
                listing.bedrooms,
                listing.bathrooms,
                listing.parking_spots,
                float(listing.price_per_m2) if listing.price_per_m2 else None,
                listing.title,
                listing.description,
                listing.scraped_at,
                scrape_run_id,
            ]
            for listing in listings
        ]
        self._db.executemany(
            """
            INSERT INTO listings (
                id, external_id, source, url, listing_type, property_type,
                city, neighborhood, street, price_brl, area_m2,
                bedrooms, bathrooms, parking_spots, price_per_m2,
                title, description, scraped_at, scrape_run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (source, external_id) DO UPDATE SET
                price_brl    = excluded.price_brl,
                price_per_m2 = excluded.price_per_m2,
                scraped_at   = excluded.scraped_at,
                is_active    = TRUE
            """,
            rows,
        )
        return len(rows)

    def query_listings(
        self,
        city: str | None = None,
        neighborhood: str | None = None,
        listing_type: str | None = None,
        property_type: str | None = None,
        scrape_run_id: str | None = None,
        limit: int = 500,
    ) -> list[dict]:
        conditions = ["is_active = TRUE"]
        params: list = []

        if city:
            conditions.append("lower(city) = lower(?)")
            params.append(city)
        if neighborhood:
            conditions.append("lower(neighborhood) = lower(?)")
            params.append(neighborhood)
        if listing_type:
            conditions.append("listing_type = ?")
            params.append(listing_type)
        if property_type:
            conditions.append("property_type = ?")
            params.append(property_type)
        if scrape_run_id:
            conditions.append("scrape_run_id = ?")
            params.append(scrape_run_id)

        where = " AND ".join(conditions)
        params.append(limit)

        rows = self._db.execute(
            f"""
            SELECT id, external_id, source, url, listing_type, property_type,
                   city, neighborhood, street, price_brl, area_m2,
                   bedrooms, bathrooms, parking_spots, price_per_m2,
                   title, scraped_at
              FROM listings
             WHERE {where}
             ORDER BY scraped_at DESC
             LIMIT ?
            """,
            params,
        ).fetchall()

        cols = ["id", "external_id", "source", "url", "listing_type", "property_type",
                "city", "neighborhood", "street", "price_brl", "area_m2",
                "bedrooms", "bathrooms", "parking_spots", "price_per_m2",
                "title", "scraped_at"]
        return [dict(zip(cols, row)) for row in rows]

    def total_listings(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM listings WHERE is_active = TRUE").fetchone()[0]

    def distinct_neighborhoods(self, city: str | None = None) -> list[str]:
        if city:
            rows = self._db.execute(
                "SELECT DISTINCT neighborhood FROM listings WHERE lower(city) = lower(?) ORDER BY 1",
                [city],
            ).fetchall()
        else:
            rows = self._db.execute(
                "SELECT DISTINCT neighborhood FROM listings ORDER BY 1"
            ).fetchall()
        return [r[0] for r in rows]

    def distinct_cities(self) -> list[str]:
        rows = self._db.execute(
            "SELECT DISTINCT city FROM listings ORDER BY 1"
        ).fetchall()
        return [r[0] for r in rows]

    def distinct_sources(self) -> list[str]:
        rows = self._db.execute(
            "SELECT DISTINCT source FROM listings WHERE is_active = TRUE ORDER BY 1"
        ).fetchall()
        return [r[0] for r in rows]

    def clear_all_listings(self) -> int:
        """Delete all listings and scrape runs. Returns count of deleted listings."""
        count = self._db.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        self._db.execute("DELETE FROM listings")
        self._db.execute("DELETE FROM scrape_runs")
        return count

    # ------------------------------------------------------------------
    # Favorites
    # ------------------------------------------------------------------

    def toggle_favorite(self, listing_id: str) -> bool:
        """Toggle is_favorite for a listing. Returns new state (True = favorited)."""
        current = self._db.execute(
            "SELECT is_favorite FROM listings WHERE id = ?", [listing_id]
        ).fetchone()
        if current is None:
            return False
        new_state = not current[0]
        self._db.execute(
            "UPDATE listings SET is_favorite = ? WHERE id = ?", [new_state, listing_id]
        )
        return new_state

    def query_favorites(self) -> list[dict]:
        """Return all favorited listings."""
        rows = self._db.execute(
            """
            SELECT id, external_id, source, url, listing_type, property_type,
                   city, neighborhood, street, price_brl, area_m2,
                   bedrooms, bathrooms, parking_spots, price_per_m2,
                   title, scraped_at
              FROM listings
             WHERE is_favorite = TRUE AND is_active = TRUE
             ORDER BY scraped_at DESC
            """
        ).fetchall()
        cols = ["id", "external_id", "source", "url", "listing_type", "property_type",
                "city", "neighborhood", "street", "price_brl", "area_m2",
                "bedrooms", "bathrooms", "parking_spots", "price_per_m2",
                "title", "scraped_at"]
        return [dict(zip(cols, row)) for row in rows]
