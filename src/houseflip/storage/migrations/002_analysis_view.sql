-- Migration 002: Neighborhood stats view

CREATE OR REPLACE VIEW neighborhood_stats AS
SELECT
    city,
    neighborhood,
    listing_type,
    property_type,
    COUNT(*)                    AS listing_count,
    MEDIAN(price_brl)           AS median_price,
    AVG(price_brl)              AS mean_price,
    STDDEV_SAMP(price_brl)      AS stddev_price,
    MEDIAN(price_per_m2)        AS median_price_per_m2,
    AVG(price_per_m2)           AS mean_price_per_m2,
    STDDEV_SAMP(price_per_m2)   AS stddev_price_per_m2
FROM listings
WHERE is_active = TRUE
  AND price_brl > 0
  AND area_m2 > 0
GROUP BY city, neighborhood, listing_type, property_type
HAVING COUNT(*) >= 3;
