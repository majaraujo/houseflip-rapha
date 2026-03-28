-- Migration 001: Initial schema

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     VARCHAR PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id            VARCHAR PRIMARY KEY,
    source        VARCHAR NOT NULL,
    city          VARCHAR NOT NULL,
    neighborhood  VARCHAR,
    listing_type  VARCHAR NOT NULL,
    property_type VARCHAR NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ,
    total_found   INTEGER,
    status        VARCHAR NOT NULL DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS listings (
    id             VARCHAR PRIMARY KEY,
    external_id    VARCHAR NOT NULL,
    source         VARCHAR NOT NULL,
    url            VARCHAR NOT NULL,
    listing_type   VARCHAR NOT NULL,
    property_type  VARCHAR NOT NULL,
    city           VARCHAR NOT NULL,
    neighborhood   VARCHAR NOT NULL,
    street         VARCHAR,
    price_brl      DECIMAL(15, 2) NOT NULL,
    area_m2        DECIMAL(10, 2),
    bedrooms       SMALLINT,
    bathrooms      SMALLINT,
    parking_spots  SMALLINT,
    price_per_m2   DECIMAL(15, 2),
    title          VARCHAR,
    description    VARCHAR,
    scraped_at     TIMESTAMPTZ NOT NULL,
    scrape_run_id  VARCHAR REFERENCES scrape_runs(id),
    is_active      BOOLEAN DEFAULT TRUE,
    UNIQUE (source, external_id)
);
