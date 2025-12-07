from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# Load location ID to state/city mapping
def load_location_to_state_city_mapping():
    """
    Load a lookup table mapping location_id to state and city from state_city_codes.csv
    
    Why this function name:
    - load = to load
    - location_to_state_city_mapping = mapping from location to state/city
    Meaning: "load the location to state/city mapping data"
    
    How to use:
    This function reads the CSV file and creates a dictionary where:
    - Key: location_id (the Code column)
    - Value: dictionary with 'state' and 'city' information
    
    Returns:
    --------
    dict: {location_id: {'state': state_name, 'city': city_name}}
    
    Example:
    {
        1671: {'state': 'Alabama', 'city': 'Montgomery'},
        1957: {'state': 'Alabama', 'city': 'Birmingham'}
    }
    """
    csv_path = Path(__file__).parent.parent / 'sensor_locations' / 'state_city_codes.csv'
    
    if not csv_path.exists():
        print(f"⚠️ Could not find state_city_codes.csv file: {csv_path}")
        return {}
    
    try:
        df = pd.read_csv(csv_path)
        # Build a dictionary mapping location_id (Code) to state and city
        mapping_dict = {}
        for _, row in df.iterrows():
            if pd.notna(row.get('Code')) and pd.notna(row.get('State')) and pd.notna(row.get('City')):
                location_id = int(row['Code'])
                mapping_dict[location_id] = {
                    'state': str(row['State']),
                    'city': str(row['City'])
                }
        
        print(f"✅ Successfully loaded {len(mapping_dict)} location-to-state/city mappings")
        return mapping_dict
    except Exception as e:
        print(f"⚠️ Error loading state_city_codes.csv file: {e}")
        return {}


def load_sensor_to_location_mapping():
    """
    Load a lookup table mapping sensor_id to location_id from sensor_to_location_mapping.csv
    
    This file is generated during data fetching and records which sensor belongs to which location.
    
    Returns:
    --------
    dict: {sensor_id: location_id}
    
    Example:
    {
        2961: 1671,   # sensor 2961 belongs to location 1671 (Montgomery, Alabama)
        3045: 1957,   # sensor 3045 belongs to location 1957 (Birmingham, Alabama)
    }
    """
    csv_path = Path(__file__).parent.parent / 'sensor_locations' / 'sensor_to_location_mapping.csv'
    
    if not csv_path.exists():
        print(f"⚠️ Could not find sensor_to_location_mapping.csv file: {csv_path}")
        print(f"   This file is auto-generated during data fetching.")
        print(f"   Please run the data pipeline first to generate this mapping.")
        return {}
    
    try:
        df = pd.read_csv(csv_path)
        # Build a dictionary mapping sensor_id to location_id
        mapping_dict = {}
        for _, row in df.iterrows():
            if pd.notna(row.get('sensor_id')) and pd.notna(row.get('location_id')):
                mapping_dict[int(row['sensor_id'])] = int(row['location_id'])
        
        print(f"✅ Successfully loaded {len(mapping_dict)} sensor-to-location mappings")
        return mapping_dict
    except Exception as e:
        print(f"⚠️ Error loading sensor_to_location_mapping.csv file: {e}")
        return {}


# Global variables: Load mappings once at startup
LOCATION_TO_STATE_CITY = load_location_to_state_city_mapping()
SENSOR_TO_LOCATION = load_sensor_to_location_mapping()


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
    
    # Two-layer mapping: sensor_id -> location_id -> state/city
    # Step 1: Get location_id from sensor_id
    df["location_id"] = df["sensor_id"].apply(
        lambda sid: SENSOR_TO_LOCATION.get(sid, sid)  # fallback to sensor_id if mapping not found
    )
    
    # Step 2: Get state and city from location_id
    # lambda lid: ... means "for each location_id (lid), do the following"
    df["state"] = df["location_id"].apply(
        lambda lid: LOCATION_TO_STATE_CITY.get(lid, {}).get('state', 'Unknown')
    )
    df["city"] = df["location_id"].apply(
        lambda lid: LOCATION_TO_STATE_CITY.get(lid, {}).get('city', f"Location {lid}")
    )
    
    # Create location_name as "City, State" format for display
    # This combines city and state into a readable format like "Montgomery, Alabama"
    df["location_name"] = df.apply(
        lambda row: f"{row['city']}, {row['state']}" if row['state'] != 'Unknown' else row['city'],
        axis=1
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
    
    # Two-layer mapping: sensor_id -> location_id -> state/city
    # Step 1: Get location_id from sensor_id
    df["location_id"] = df["sensor_id"].apply(
        lambda sid: SENSOR_TO_LOCATION.get(sid, sid)  # fallback to sensor_id if mapping not found
    )
    
    # Step 2: Get state and city from location_id
    df["state"] = df["location_id"].apply(
        lambda lid: LOCATION_TO_STATE_CITY.get(lid, {}).get('state', 'Unknown')
    )
    df["city"] = df["location_id"].apply(
        lambda lid: LOCATION_TO_STATE_CITY.get(lid, {}).get('city', f"Location {lid}")
    )
    
    # Create location_name as "City, State" format for display
    df["location_name"] = df.apply(
        lambda row: f"{row['city']}, {row['state']}" if row['state'] != 'Unknown' else row['city'],
        axis=1
    )
    
    return df
