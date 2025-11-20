-- init_db/01_create_tables.sql
-- This SQL file is used to initialize the database table schema

-- Main measurement data table: Stores daily PM2.5 data fetched from OpenAQ sensors
-- Each row represents a PM2.5 measurement value from a sensor at a specific time
CREATE TABLE IF NOT EXISTS sensor_data (
    id SERIAL PRIMARY KEY,                          -- Auto-increment primary key
    sensor_id INTEGER NOT NULL,                     -- Sensor ID
    parameter VARCHAR(50) NOT NULL,                 -- Parameter name (fixed as 'pm25')
    datetime_utc TIMESTAMP NOT NULL,                -- UTC time
    datetime_local TIMESTAMP NOT NULL,              -- Local time
    value NUMERIC,                                  -- Measurement value (µg/m³)
    units VARCHAR(20),                              -- Unit
    coverage_percent NUMERIC,                       -- Data coverage percentage
    min NUMERIC,                                    -- Daily minimum value
    max NUMERIC,                                    -- Daily maximum value
    median NUMERIC,                                 -- Daily median value
    latitude NUMERIC,                               -- Latitude coordinate
    longitude NUMERIC,                              -- Longitude coordinate
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Data creation time
    UNIQUE(sensor_id, parameter, datetime_utc)      -- Ensure no duplicate for same sensor, parameter, and time
);
