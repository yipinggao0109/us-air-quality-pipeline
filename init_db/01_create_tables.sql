-- init_db/01_create_tables.sql

-- Drop existing table if you want to recreate it
DROP TABLE IF EXISTS sensor_data CASCADE;

-- Create sensor_data table matching your CSV structure
CREATE TABLE IF NOT EXISTS sensor_data (
    id SERIAL PRIMARY KEY,
    sensor_id INTEGER NOT NULL,
    parameter VARCHAR(50) NOT NULL,
    datetime_utc TIMESTAMP NOT NULL,
    datetime_local TIMESTAMP NOT NULL,
    value NUMERIC,
    units VARCHAR(20),
    coverage_percent NUMERIC,
    min NUMERIC,
    max NUMERIC,
    median NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Prevent duplicate records
    UNIQUE(sensor_id, parameter, datetime_utc)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_sensor_id ON sensor_data(sensor_id);
CREATE INDEX IF NOT EXISTS idx_parameter ON sensor_data(parameter);
CREATE INDEX IF NOT EXISTS idx_datetime_utc ON sensor_data(datetime_utc);
CREATE INDEX IF NOT EXISTS idx_datetime_local ON sensor_data(datetime_local);
CREATE INDEX IF NOT EXISTS idx_sensor_datetime ON sensor_data(sensor_id, datetime_utc);

-- Create a view for easy data exploration
CREATE OR REPLACE VIEW sensor_data_summary AS
SELECT 
    sensor_id,
    parameter,
    COUNT(*) as record_count,
    MIN(datetime_utc) as earliest_record,
    MAX(datetime_utc) as latest_record,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    AVG(coverage_percent) as avg_coverage
FROM sensor_data
GROUP BY sensor_id, parameter
ORDER BY sensor_id, parameter;

-- Create a view for recent data
CREATE OR REPLACE VIEW recent_sensor_readings AS
SELECT 
    sensor_id,
    parameter,
    datetime_local,
    value,
    units,
    coverage_percent
FROM sensor_data
WHERE datetime_utc >= NOW() - INTERVAL '7 days'
ORDER BY datetime_utc DESC;