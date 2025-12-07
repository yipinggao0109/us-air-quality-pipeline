from openaq import OpenAQ
import pandas as pd
import os
from dotenv import load_dotenv
from dateutil import parser 
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from pathlib import Path

# Load environment variables
load_dotenv()

def load_location_coordinates():
    """
    Load a lookup table mapping location_id to coordinates from openaq_us_locations_enriched.csv

    Returns:
    --------
    dict: {location_id: {'latitude': float, 'longitude': float}}
    """
    csv_path = Path(__file__).parent.parent.parent / 'frontend' / 'sensor_locations' / 'openaq_us_locations_enriched.csv'
    
    if not csv_path.exists():
        print(f"⚠️ Warning: Could not find coordinates lookup file {csv_path}")
        return {}
    
    try:
        df = pd.read_csv(csv_path)
        # Build dictionary mapping location_id to coordinates
        coords_dict = {}
        for _, row in df.iterrows():
            if pd.notna(row.get('location_id')) and pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
                coords_dict[int(row['location_id'])] = {
                    'latitude': float(row['latitude']),
                    'longitude': float(row['longitude'])
                }
        
        print(f"✅ Successfully loaded coordinates for {len(coords_dict)} locations")
        return coords_dict
    except Exception as e:
        print(f"⚠️ Error loading coordinates lookup file: {e}")
        return {}

# Global variable: Load the coordinates lookup table once
LOCATION_COORDS = load_location_coordinates()

def get_utc_date_today():
    """
    Returns today's date in UTC as a timezone-aware datetime.date object.
    """
    utc_now = datetime.now(timezone.utc)
    return utc_now.date()


def db_table_exists(table_name: str = "sensor_data") -> bool:
    """
    Check if a given table exists in the current database.
    """
    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT EXISTS (
                      SELECT 1
                      FROM information_schema.tables
                      WHERE table_schema = 'public'
                        AND table_name = :table_name
                    );
                """),
                {"table_name": table_name},
            )
            return result.scalar()
    finally:
        engine.dispose()


def initialize_db(sql_path: str = "init_db/01_create_tables.sql", table_name: str = "sensor_data"):
    """
    Ensure the DB schema is created. If the main table doesn't exist,
    run the SQL in `sql_path` against Supabase.
    """
    if db_table_exists(table_name):
        print(f"Table {table_name} already exists – skipping init.")
        return

    sql_file = Path(sql_path)
    if not sql_file.exists():
        raise FileNotFoundError(f"Init SQL file not found: {sql_path}")

    sql = sql_file.read_text()

    engine = get_db_engine()
    try:
        with engine.begin() as conn:
            # exec_driver_sql lets you run multi-statement SQL from a file
            conn.exec_driver_sql(sql)
        print(f"Initialized database using {sql_path}")
    finally:
        engine.dispose()


def get_db_engine():
    """
    Create and return a SQLAlchemy engine for PostgreSQL (Supabase) connection.
    """
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB')

    if not all([user, password, host, port, database]):
        raise RuntimeError("Database env vars not fully set (POSTGRES_*)")

    # Supabase requires SSL
    connection_string = (
        f"postgresql+psycopg2://{user}:{password}"
        f"@{host}:{port}/{database}?sslmode=require"
    )

    engine = create_engine(connection_string, pool_pre_ping=True)
    return engine

def parse_date_to_openaq_format(date_input):
    """
    Convert various date formats to OpenAQ API format (ISO 8601 with Z).
    
    Parameters:
    -----------
    date_input : str, datetime, or None
        Date in various formats:
        - "1/1/2020", "01/01/2020", "2020-1-1"
        - "January 1, 2020", "Jan 1 2020"
        - "2020-01-01", "2020/01/01"
        - datetime object
        - None (returns None)
    
    Returns:
    --------
    str
        Date in format "YYYY-MM-DDTHH:MM:SSZ"
        
    Examples:
    ---------
    >>> parse_date_to_openaq_format("1/1/2020")
    '2020-01-01T00:00:00Z'
    
    >>> parse_date_to_openaq_format("January 15, 2023")
    '2023-01-15T00:00:00Z'
    
    >>> parse_date_to_openaq_format("2024-12-31")
    '2024-12-31T00:00:00Z'
    """

    if date_input is None:
        return None
    
    # If already a datetime object
    if isinstance(date_input, datetime):
        return date_input.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # If already in correct format, return as-is
    if isinstance(date_input, str) and date_input.endswith('Z'):
        return date_input
    
    try:
        # Use dateutil.parser to handle various formats
        # dayfirst=False means 1/2/2020 = Jan 2, not Feb 1 (US format)
        dt = parser.parse(date_input, dayfirst=False)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    except Exception as e:
        raise ValueError(f"Could not parse date '{date_input}'. Error: {e}")


def get_pm25_sensors_from_location(client, location_id):
    """
    Get all PM2.5 sensors from a specified location

    This function follows the location-based logic from fetch_data_to_csv.py (lines 344-372):
    1. Get the detailed information for the location
    2. Extract all sensors from the location
    3. Filter for PM2.5 sensors

    Parameters:
    -----------
    client : OpenAQ
        OpenAQ API client instance
    location_id : int
        Location ID to query

    Returns:
    --------
    list
        List of PM2.5 sensor objects from this location
        
    Usage:
    ------
    pm25_sensors = get_pm25_sensors_from_location(client, 1671)
    for sensor in pm25_sensors:
        print(f"Sensor ID: {sensor.id}, Name: {sensor.name}")
    """
    try:
        # Get detailed location info (including all sensors)
        location_resp = client.locations.get(location_id)
        location_obj = (location_resp.results or [None])[0]
        
        if location_obj is None:
            print(f"  ⚠️  Location {location_id} returned no metadata")
            return []
        
        # Extract all sensors
        sensors = getattr(location_obj, "sensors", []) or []
        print(f"  Found {len(sensors)} total sensors at location {location_id}")
        
        # Filter for PM2.5 sensors (as in fetch_data_to_csv.py lines 358-364)
        pm25_sensors = [
            sensor
            for sensor in sensors
            if getattr(sensor.parameter, "name", "").lower() == "pm25"
        ]
        
        print(f"  After filtering for PM2.5: {len(pm25_sensors)} sensors")
        
        return pm25_sensors
    
    except Exception as e:
        print(f"  ⚠️  Could not retrieve location {location_id}: {e}")
        return []


def fetch_sensor_measurements(client, sensor, datetime_from, datetime_to, location_id=None):
    """
    Fetch measurements for a single sensor

    This function follows the logic from fetch_data_to_csv.py (lines 183-252)

    Parameters:
    -----------
    client : OpenAQ
        OpenAQ API client instance
    sensor : object
        Sensor object from OpenAQ API (taken from location.sensors)
    datetime_from : str
        Start date in OpenAQ format
    datetime_to : str
        End date in OpenAQ format
    location_id : int, optional
        Location ID, used for looking up coordinates in CSV

    Returns:
    --------
    pd.DataFrame
        DataFrame with measurement data
    """
    start = parse_date_to_openaq_format(datetime_from)
    end = parse_date_to_openaq_format(datetime_to)
    
    records = []
    page = 1
    
    sensor_id = sensor.id
    
    # Prefer to get coordinates from CSV mapping (most reliable)
    sensor_lat = None
    sensor_lon = None
    
    if location_id and location_id in LOCATION_COORDS:
        sensor_lat = LOCATION_COORDS[location_id]['latitude']
        sensor_lon = LOCATION_COORDS[location_id]['longitude']
        print(f"  ↳ Fetching sensor {sensor_id} ({sensor.name}) [CSV coords: lat={sensor_lat}, lon={sensor_lon}]")
    else:
        # Fallback: get coordinates from sensor object
        sensor_coords = getattr(sensor, 'coordinates', None)
        sensor_lat = getattr(sensor_coords, 'latitude', None) if sensor_coords else None
        sensor_lon = getattr(sensor_coords, 'longitude', None) if sensor_coords else None
        print(f"  ↳ Fetching sensor {sensor_id} ({sensor.name}) [API coords: lat={sensor_lat}, lon={sensor_lon}]")
    
    while True:
        try:
            response = client.measurements.list(
                sensors_id=sensor_id,
                datetime_from=start,
                datetime_to=end,
                data="days",
                limit=1000,
                page=page,
            )
        except Exception as exc:
            print(f"    ⚠️  Error on page {page}: {exc}")
            break
        
        if not response.results:
            break
        
        # Process each measurement record (using sensor coordinates)
        for result in response.results:
            period = getattr(result, "period", None)
            datetime_from_obj = getattr(period, "datetime_from", None)
            
            records.append({
                'sensor_id': sensor_id,
                'parameter': getattr(result.parameter, "name", "pm25"),
                'datetime_utc': getattr(datetime_from_obj, "utc", None),
                'datetime_local': getattr(datetime_from_obj, "local", None),
                'value': result.value,
                'units': getattr(result.parameter, "units", None),
                'coverage_percent': getattr(getattr(result, "coverage", None), "percent_complete", None),
                'min': getattr(getattr(result, "summary", None), "min", None),
                'max': getattr(getattr(result, "summary", None), "max", None),
                'median': getattr(getattr(result, "summary", None), "median", None),
                'latitude': sensor_lat,   # Use sensor coordinates
                'longitude': sensor_lon,  # Use sensor coordinates
            })
        
        print(f"    Page {page}: {len(response.results)} records")
        
        if len(response.results) < 1000:
            break
        
        page += 1
    
    df = pd.DataFrame(records)
    
    # Key: Client-side date filtering (same as fetch_data_to_csv.py lines 241-249)
    if not df.empty:
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
        start_dt = pd.to_datetime(start) if start else None
        end_dt = pd.to_datetime(end) if end else None
        
        if start_dt is not None:
            df = df[df["datetime_utc"] >= start_dt]
        if end_dt is not None:
            df = df[df["datetime_utc"] <= end_dt]
        
        # Convert back to string format
        df["datetime_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    print(f"    Total collected for sensor {sensor_id}: {len(df)} rows")
    
    return df


def prepare_dataframe_for_db(df):
    """
    Prepare a DataFrame before inserting into the database, ensuring correct data types.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Original DataFrame retrieved from the API
    
    Returns:
    --------
    pd.DataFrame
        Cleaned DataFrame ready for database insertion
        
    Purpose of this function:
    - prepare = prepare
    - dataframe = pandas data structure
    - for_db = for database use
    That is, "prepare DataFrame for database insertion"
    
    It ensures:
    1. datetime columns are in correct datetime format
    2. numerical columns are of numeric type
    3. string columns are string type
    """
    if df.empty:
        return df
    
    df_clean = df.copy()
    
    # Convert datetime columns to correct datetime format
    # Just like in fetch_data_to_csv.py, ensure correct datetime processing
    if 'datetime_utc' in df_clean.columns:
        df_clean['datetime_utc'] = pd.to_datetime(df_clean['datetime_utc'], utc=True)
    
    if 'datetime_local' in df_clean.columns:
        df_clean['datetime_local'] = pd.to_datetime(df_clean['datetime_local'])
    
    # Ensure numeric columns are of correct numeric type
    # 'coerce' parameter will set invalid values to NaN
    numeric_columns = ['sensor_id', 'value', 'coverage_percent', 'min', 'max', 'median', 'latitude', 'longitude']
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Ensure string columns are string type
    string_columns = ['parameter', 'units']
    for col in string_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
    
    return df_clean


def save_sensor_to_location_mapping(sensor_to_location_map, output_path=None):
    """
    Save sensor_id to location_id mapping to a CSV file
    
    This mapping is crucial for the frontend to correctly map sensor data to location information
    (state/city names) from state_city_codes.csv
    
    Parameters:
    -----------
    sensor_to_location_map : dict
        Dictionary mapping sensor_id -> location_id
    output_path : str, optional
        Path to save the CSV file. If None, saves to src/frontend/sensor_locations/
    
    Returns:
    --------
    str
        Path to the saved CSV file
    """
    if not sensor_to_location_map:
        print("⚠️  No sensor-location mappings to save")
        return None
    
    # Default output path: save to frontend/sensor_locations/
    if output_path is None:
        base_dir = Path(__file__).parent.parent.parent / 'frontend' / 'sensor_locations'
        base_dir.mkdir(parents=True, exist_ok=True)
        output_path = base_dir / 'sensor_to_location_mapping.csv'
    else:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert mapping to DataFrame
    mapping_df = pd.DataFrame([
        {'sensor_id': sensor_id, 'location_id': location_id}
        for sensor_id, location_id in sensor_to_location_map.items()
    ])
    
    # Sort by sensor_id for readability
    mapping_df = mapping_df.sort_values('sensor_id').reset_index(drop=True)
    
    # Save to CSV
    mapping_df.to_csv(output_path, index=False)
    
    print(f"\n✅ Saved {len(mapping_df)} sensor-location mappings to: {output_path}")
    
    return str(output_path)


def save_dataframe_to_db(df, table_name='sensor_data', if_exists='append'):
    """
    Save a pandas DataFrame to PostgreSQL (Supabase) using a single
    bulk INSERT ... ON CONFLICT DO NOTHING.

    - New rows are inserted.
    - Rows that would violate the unique constraint are skipped.
    - Duplicates *within* df are removed before sending to DB.

    Returns
    -------
    dict: {attempted, inserted, duplicates}
    """
    if df.empty:
        print("⚠ DataFrame is empty, nothing to insert")
        return {'attempted': 0, 'inserted': 0, 'duplicates': 0}

    # Clean + enforce types once, vectorized
    df_clean = prepare_dataframe_for_db(df)

    # Drop exact duplicates on the key used for ON CONFLICT
    key_cols = ['sensor_id', 'parameter', 'datetime_utc']
    for c in key_cols:
        if c not in df_clean.columns:
            raise ValueError(f"Missing key column '{c}' in DataFrame")
    df_clean = df_clean.drop_duplicates(subset=key_cols)

    # Only keep the columns we need to insert, in the correct order
    cols = [
        'sensor_id', 'parameter', 'datetime_utc', 'datetime_local',
        'value', 'units', 'coverage_percent', 'min', 'max', 'median',
        'latitude', 'longitude'
    ]
    missing = [c for c in cols if c not in df_clean.columns]
    if missing:
        raise ValueError(f"Missing columns for insert: {missing}")

    # Quickly convert to list-of-dicts (better than using iterrows, more efficient)
    records = df_clean[cols].to_dict(orient='records')

    if not records:
        print("⚠ After de-duplication, nothing to insert")
        return {'attempted': 0, 'inserted': 0, 'duplicates': 0}

    engine = get_db_engine()

    try:
        insert_sql = f"""
            INSERT INTO {table_name}
            (sensor_id, parameter, datetime_utc, datetime_local, value, units,
             coverage_percent, min, max, median, latitude, longitude)
            VALUES
            (:sensor_id, :parameter, :datetime_utc, :datetime_local, :value, :units,
             :coverage_percent, :min, :max, :median, :latitude, :longitude)
            ON CONFLICT (sensor_id, parameter, datetime_utc) DO NOTHING
        """

        with engine.begin() as conn:
            result = conn.execute(text(insert_sql), records)
            inserted = result.rowcount   # rows actually inserted

        attempted = len(records)
        duplicates = attempted - inserted
        print(f"✓ Inserted {inserted} new rows, skipped {duplicates} duplicates")

        return {
            'attempted': attempted,
            'inserted': inserted,
            'duplicates': duplicates
        }

    except Exception as e:
        print(f"✗ Error saving to database: {e}")
        raise
    finally:
        engine.dispose()

def fetch_and_save_to_db(client, location_ids, datetime_from, datetime_to, table_name='sensor_data'):
    """
    Fetch PM2.5 data from multiple locations and save to database

    This function fully follows the location-based method in fetch_data_to_csv.py (lines 338-408):
    1. Iterate through each location ID
    2. Get all sensors from the location
    3. Filter for PM2.5 sensors
    4. For each PM2.5 sensor, fetch measurement data
    5. Save to the database
    6. Record sensor_id to location_id mapping

    Parameters:
    -----------
    client : OpenAQ
        OpenAQ API client instance
    location_ids : list
        List of location IDs (read from states_codes.csv for all 50 states)
    datetime_from : str
        Start date
    datetime_to : str
        End date
    table_name : str
        Target database table name

    Returns:
    --------
    dict
        Summary statistics
    """
    successful_sensors = 0
    failed_sensors = 0
    skipped_locations = 0
    total_rows_attempted = 0
    total_rows_inserted = 0
    total_duplicates = 0
    
    # Dictionary to track sensor_id -> location_id mapping
    sensor_to_location_map = {}
    
    for idx, location_id in enumerate(location_ids, start=1):
        print(f"\n[{idx}/{len(location_ids)}] Location {location_id}")
        print("-" * 70)
        
        try:
            # Get all PM2.5 sensors from location (as in fetch_data_to_csv.py)
            pm25_sensors = get_pm25_sensors_from_location(client, location_id)
            
            if not pm25_sensors:
                print(f"  No PM2.5 sensors found at location {location_id}")
                skipped_locations += 1
                continue
            
            # Fetch data for each PM2.5 sensor
            for sensor in pm25_sensors:
                try:
                    sensor_id = sensor.id
                    
                    # Record the sensor_id to location_id mapping
                    sensor_to_location_map[sensor_id] = location_id
                    
                    df = fetch_sensor_measurements(
                        client=client,
                        sensor=sensor,
                        datetime_from=datetime_from,
                        datetime_to=datetime_to,
                        location_id=location_id  # Pass location_id for coordinates lookup
                    )
                    
                    if df.empty:
                        print(f"  ⚠️  No measurements for sensor {sensor.id}")
                        failed_sensors += 1
                        continue
                    
                    # Save to database
                    result = save_dataframe_to_db(df, table_name=table_name)
                    
                    total_rows_attempted += result.get('attempted', 0)
                    total_rows_inserted += result.get('inserted', 0)
                    total_duplicates += result.get('duplicates', 0)
                    successful_sensors += 1
                    
                    print(f"  ✅ Saved sensor {sensor.id} to database")
                    
                except Exception as e:
                    print(f"  ✗ Error processing sensor {sensor.id}: {e}")
                    failed_sensors += 1
                    
        except Exception as e:
            print(f"  ✗ Error processing location {location_id}: {e}")
            skipped_locations += 1
    
    # Save sensor-to-location mapping to CSV
    save_sensor_to_location_mapping(sensor_to_location_map)
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Locations processed: {len(location_ids) - skipped_locations}/{len(location_ids)}")
    print(f"Successful PM2.5 sensors: {successful_sensors}")
    print(f"Failed sensors: {failed_sensors}")
    print(f"Skipped locations: {skipped_locations}")
    print(f"Sensor-Location mappings recorded: {len(sensor_to_location_map)}")
    print(f"Rows attempted: {total_rows_attempted}")
    print(f"Rows inserted: {total_rows_inserted}")
    print(f"Duplicates skipped: {total_duplicates}")
    
    return {
        'successful_sensors': successful_sensors,
        'failed_sensors': failed_sensors,
        'skipped_locations': skipped_locations,
        'total_locations': len(location_ids),
        'sensor_mappings': len(sensor_to_location_map),
        'total_rows_attempted': total_rows_attempted,
        'total_rows_inserted': total_rows_inserted,
        'total_duplicates': total_duplicates
    }

def test_db_connection():
    """
    Test the database connection.
    
    Returns:
    --------
    bool
        True if connection successful, False otherwise
    """
    engine = get_db_engine()
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print(f"Successfully connected to PostgreSQL")
            print(f"Database version: {version}")
            return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False
    finally:
        engine.dispose()