from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# Load location name lookup table
def load_location_names():
    """
    Load a lookup table mapping location_id to location_name from openaq_us_locations_enriched.csv

    Returns:
    --------
    dict: {location_id: location_name}
    """
    csv_path = Path(__file__).parent.parent / 'sensor_locations' / 'openaq_us_locations_enriched.csv'
    
    if not csv_path.exists():
        print(f"⚠️ Could not find location name lookup file: {csv_path}")
        return {}
    
    try:
        df = pd.read_csv(csv_path)
        # Build a dictionary mapping location_id to name
        names_dict = {}
        for _, row in df.iterrows():
            if pd.notna(row.get('location_id')) and pd.notna(row.get('name')):
                names_dict[int(row['location_id'])] = str(row['name'])
        
        print(f"✅ Successfully loaded {len(names_dict)} location names")
        return names_dict
    except Exception as e:
        print(f"⚠️ Error loading location name lookup file: {e}")
        return {}

# Global variable: Load location names once
LOCATION_NAMES = load_location_names()


@contextmanager
def db_engine() -> Iterator:
    """
    Create a context manager for database connections.

    Purpose of this function:
    - db_engine means "database engine"
    - The @contextmanager decorator allows this function to be used with a with statement

    Example usage:
    with db_engine() as engine:
        # Use engine to perform database operations here
        df = pd.read_sql(query, engine)
    # The connection is automatically closed when leaving the with block

    Why use a context manager:
    - Automatically manage resources and ensure connections are closed properly after use
    - Even if an error occurs resources will be cleaned up
    - Cleaner, more readable code
    """
    # Read PostgreSQL connection details from environment variables
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    sslmode = os.getenv("POSTGRES_SSLMODE", "require")

    # Check that all required environment variables are set
    if not all([host, port, database, user, password]):
        raise RuntimeError("PostgreSQL environment variables are incomplete")

    # Build connection string
    # Format: postgresql+psycopg2://user:password@host:port/database?sslmode=require
    url = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        f"?sslmode={sslmode}"
    )
    
    # Create SQLAlchemy engine
    # pool_pre_ping=True tests connection before using it
    engine = create_engine(url, pool_pre_ping=True)
    try:
        yield engine  # Pass the engine to the caller
    finally:
        engine.dispose()  # Clean up connection pool, release resources


def load_measurements(parameter: str = "pm25") -> pd.DataFrame:
    """
    Load measurement data from the database.

    Args:
    - parameter: Parameter type to query (default is "pm25" for PM2.5)

    Returns:
    - pd.DataFrame: DataFrame containing the measurement data

    Purpose of this function:
    - load = to load
    - measurements = measurement values
    Meaning: "load measurement data"

     Example usage:
     df = load_measurements(parameter="pm25")
     This loads all PM2.5 data from the sensor_data table
     """
    with db_engine() as engine:
        query = text("""
            SELECT sensor_id,
                   parameter,
                   datetime_local,
                   datetime_utc,
                   value,
                   units,
                   latitude,
                   longitude
            FROM sensor_data
            WHERE parameter = :parameter
            ORDER BY datetime_utc
            """)
        # Use SQLAlchemy to execute the query
        with engine.connect() as conn:
            result = conn.execute(query, {"parameter": parameter})
            # Convert the result to a DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

    if df.empty:
        return df

    # Convert time columns to proper datetime format
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df["datetime_local"] = pd.to_datetime(df["datetime_local"], utc=False)
    
    # Extract just the date part from datetime_utc for daily aggregation
    df["date"] = df["datetime_utc"].dt.date
    
    # Create location_id and location_name columns (as expected by frontend)
    df["location_id"] = df["sensor_id"]
    
    # Map real location names from the CSV lookup table
    df["location_name"] = df["sensor_id"].apply(
        lambda sid: LOCATION_NAMES.get(sid, f"Sensor {sid}")
    )
    
    return df


def load_sensor_metadata() -> pd.DataFrame:
    """
    Load sensor metadata from the database.

    Returns:
    - pd.DataFrame: DataFrame containing basic information for each sensor

    Purpose of this function:
    - load = to load
    - sensor_metadata = sensor metadata
    Meaning: "load basic information about each sensor"

    Example usage:
    df = load_sensor_metadata()
    This aggregates basic info for each sensor from the sensor_data table
    """
    with db_engine() as engine:
        query = text("""
            SELECT 
                sensor_id,
                parameter,
                AVG(latitude) as latitude,
                AVG(longitude) as longitude,
                MAX(datetime_utc) as last_seen
            FROM sensor_data
            GROUP BY sensor_id, parameter
            ORDER BY sensor_id
            """)
        # Use SQLAlchemy to execute the query
        with engine.connect() as conn:
            result = conn.execute(query)
            # Convert the result to a DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

    if df.empty:
        return df

    # Convert last_seen to UTC datetime format
    df["last_seen"] = pd.to_datetime(df["last_seen"], utc=True)
    
    # Map real location names from the CSV lookup table
    df["location_name"] = df["sensor_id"].apply(
        lambda sid: LOCATION_NAMES.get(sid, f"Sensor {sid}")
    )
    
    return df
