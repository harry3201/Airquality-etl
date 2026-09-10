-- Air Quality India ETL — Schema creation
-- Creates a dedicated schema so the project is isolated within a shared database.

CREATE SCHEMA IF NOT EXISTS air_quality;

SET search_path TO air_quality, public;
