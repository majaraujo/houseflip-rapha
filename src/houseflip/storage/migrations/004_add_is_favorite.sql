-- Migration 004: Add is_favorite column to listings
ALTER TABLE listings ADD COLUMN is_favorite BOOLEAN DEFAULT FALSE;
