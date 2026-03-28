-- Migration 004: Add is_favorite column to listings
ALTER TABLE listings ADD COLUMN IF NOT EXISTS is_favorite BOOLEAN NOT NULL DEFAULT FALSE;
