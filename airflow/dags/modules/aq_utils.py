import os
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pathlib import Path

import pandas as pd
from dateutil import parser
from openaq import OpenAQ
from sqlalchemy import create_engine, text

# Load location latitude/longitude mapping table
def load_location_coordinates():
    """
    Load mapping table of location_id and latitude/longitude from openaq_us_locations_enriched.csv

    Returns:
    --------
    dict: {location_id: {'latitude': float, 'longitude': float}}
    """
    # Airflow container paths may differ, try several possible paths
    possible_paths = [
        Path('/opt/airflow/src/frontend/sensor_locations/openaq_us_locations_enriched.csv'),
        Path(__file__).parent.parent.parent.parent / 'src' / 'frontend' / 'sensor_locations' / 'openaq_us_locations_enriched.csv',
    ]

    csv_path = None
    for path in possible_paths:
        if path.exists():
            csv_path = path
            break

    if not csv_path:
        print(f"⚠️ Warning: Could not find coordinates mapping CSV file")
        return {}

    try:
        df = pd.read_csv(csv_path)
        coords_dict = {}
        for _, row in df.iterrows():
            if pd.notna(row.get('location_id')) and pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
                coords_dict[int(row['location_id'])] = {
                    'latitude': float(row['latitude']),
                    'longitude': float(row['longitude'])
                }
        print(f"✅ Airflow: Successfully loaded latitude/longitude info for {len(coords_dict)} locations")
        return coords_dict
    except Exception as e:
        print(f"⚠️ Airflow: Error loading coordinates mapping CSV: {e}")
        return {}

# Global variable: Load coordinates mapping table only once
LOCATION_COORDS = load_location_coordinates()

# Read the location IDs of the 50 US states from states_codes.csv
# These IDs correspond to a representative monitoring station in each state
# Each location can have multiple sensors; we'll filter for PM2.5 sensors
import csv
from pathlib import Path

# Use fallback list due to Docker container path structure
# This is more reliable and avoids path problems
DEFAULT_LOCATION_IDS = [
    1671, 1404, 564, 2045, 8830, 2183, 2151, 1149, 1560, 1951,  # Alabama - Georgia
    2090, 1398, 8864, 314, 1619, 1155, 1897, 1754, 1448, 1142,  # Hawaii - Maryland
    452, 7003, 1772, 326, 1626, 512, 3837, 1634, 1028, 962,     # Massachusetts - New Jersey
    1985, 654, 1801, 1689, 1944, 2027, 2030, 1116, 557, 807,    # New Mexico - South Carolina
    1037, 289, 1316, 288, 1590, 1615, 1275, 1650, 1601, 9510    # South Dakota - Wyoming
]

print(f"✅ Airflow: Using 50 state location IDs (extracted from states_codes.csv)")

# Set the default start date to 2024/01/01
DEFAULT_START_DATE = "2024-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# Environment + configuration helpers
# ---------------------------------------------------------------------------

def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var {name}")
    return value


def get_engine():
    host = _required_env("POSTGRES_HOST")
    port = _required_env("POSTGRES_PORT")
    db = _required_env("POSTGRES_DB")
    user = _required_env("POSTGRES_USER")
    pw = _required_env("POSTGRES_PASSWORD")
    sslmode = os.getenv("POSTGRES_SSLMODE", "require")

    url = (
        f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
        f"?sslmode={sslmode}"
    )
    return create_engine(url, pool_pre_ping=True)


def get_location_ids() -> List[int]:
    """
    Get the list of location IDs to process

    Priority:
    1. From the environment variable OPENAQ_LOCATION_IDS
    2. If not set, use DEFAULT_LOCATION_IDS (50 representative locations from states_codes.csv)

    Returns:
    --------
    List[int]
        List of location IDs to process
    """
    raw = os.getenv("OPENAQ_LOCATION_IDS")
    if not raw:
        return DEFAULT_LOCATION_IDS

    location_ids: List[int] = []
    for token in raw.split(','):
        token = token.strip()
        if not token:
            continue
        try:
            location_ids.append(int(token))
        except ValueError:
            raise ValueError(f"Invalid location id '{token}' in OPENAQ_LOCATION_IDS")
    return location_ids


def get_default_start_date() -> str:
    return os.getenv("OPENAQ_DEFAULT_START_DATE", DEFAULT_START_DATE)


def get_region_overrides() -> Dict[int, str]:
    raw = os.getenv("SENSOR_REGION_OVERRIDES", "").strip()
    if not raw:
        return {}

    overrides: Dict[int, str] = {}
    for token in raw.split(','):
        token = token.strip()
        if not token or ':' not in token:
            continue
        sensor_part, region_part = token.split(':', 1)
        try:
            overrides[int(sensor_part.strip())] = region_part.strip()
        except ValueError:
            continue
    return overrides


# ---------------------------------------------------------------------------
# OpenAQ helpers
# ---------------------------------------------------------------------------

def get_openaq_client() -> OpenAQ:
    api_key = _required_env("OPENAQ_API_KEY")
    return OpenAQ(api_key=api_key)


def is_pm25_sensor(client: OpenAQ, sensor_id: int) -> bool:
    """
    Check if the given sensor is a PM2.5 sensor

    This function follows the approach used in fetch_data_to_csv.py:
    Check the sensor's parameter type before making API data requests

    This is equivalent to lines 358-364 of fetch_data_to_csv.py,
    which filters for PM2.5 sensors.

    Parameters:
    -----------
    client : OpenAQ
        OpenAQ API client instance
    sensor_id : int
        Sensor ID to check
    
    Returns:
    --------
    bool
        True if sensor measures PM2.5, False otherwise
    """
    try:
        response = client.sensors.get(sensor_id)
        if not response.results:
            return False

        sensor = response.results[0]
        if not hasattr(sensor, 'parameter') or not sensor.parameter:
            return False

        # sensor.parameter could be a dict or object, handle both
        if isinstance(sensor.parameter, dict):
            param_name = sensor.parameter.get('name', '').lower().replace('.', '').replace('_', '')
        else:
            param_name = sensor.parameter.name.lower().replace('.', '').replace('_', '')

        return 'pm25' in param_name or 'pm2' in param_name

    except Exception as e:
        print(f"  ⚠️  Could not check sensor {sensor_id}: {e}")
        return False


def get_pm25_sensors_from_location(client: OpenAQ, location_id: int) -> List:
    """
    Get all PM2.5 sensors from the given location

    This function follows the location-based logic of fetch_data_to_csv.py (lines 344-372):
    1. Get location details
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
    List
        List of PM2.5 sensor objects from this location
    """
    try:
        # Get location details (including all sensors)
        location_resp = client.locations.get(location_id)
        location_obj = (location_resp.results or [None])[0]

        if location_obj is None:
            print(f"  ⚠️  Location {location_id} returned no metadata")
            return []

        # Extract all sensors
        sensors = getattr(location_obj, "sensors", []) or []
        print(f"  Found {len(sensors)} total sensors at location {location_id}")

        # Filter for PM2.5 sensors (same logic as fetch_data_to_csv.py 358-364)
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


def parse_date_to_openaq_format(date_input):
    if date_input is None:
        return None

    if isinstance(date_input, datetime):
        return date_input.strftime("%Y-%m-%dT%H:%M:%SZ")

    date_str = str(date_input).strip()
    if date_str.endswith('Z'):
        return date_str

    parsed = parser.parse(date_str)
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")


def _measurement_to_record(sensor_id: int, measurement, sensor_lat=None, sensor_lon=None) -> dict:
    # Prefer supplied sensor latitude/longitude, otherwise try to get from measurement object
    coords = measurement.coordinates if hasattr(measurement, 'coordinates') else None
    latitude = sensor_lat if sensor_lat is not None else (getattr(coords, 'latitude', None) if coords else None)
    longitude = sensor_lon if sensor_lon is not None else (getattr(coords, 'longitude', None) if coords else None)

    return {
        'sensor_id': sensor_id,
        'parameter': 'pm25',  # Always use 'pm25' as the parameter name
        'datetime_utc': measurement.period.datetime_from.utc,
        'datetime_local': measurement.period.datetime_from.local,
        'value': measurement.value,
        'units': measurement.parameter.units,
        'coverage_percent': measurement.coverage.percent_complete if measurement.coverage else None,
        'min': measurement.summary.min if measurement.summary else None,
        'max': measurement.summary.max if measurement.summary else None,
        'median': measurement.summary.median if measurement.summary else None,
        'latitude': latitude,
        'longitude': longitude,
    }


def fetch_sensor_measurements(client: OpenAQ, sensor_id: int, datetime_from: str, datetime_to: str,
                             sensor_lat=None, sensor_lon=None, location_id=None) -> pd.DataFrame:
    """
    Fetch measurements for a given sensor from the OpenAQ API

    ⚠️ Important: This function assumes you've already confirmed externally that the sensor is a PM2.5 sensor.
    It's recommended to use is_pm25_sensor() to check before calling this function.

    Following the fetch_data_to_csv.py strategy:
    Do not filter again at the measurement data level as we've already ensured at the sensor level it's PM2.5.

    Parameters:
    -----------
    sensor_lat, sensor_lon: Optional sensor lat/lon (from the sensor object)
    location_id: Optional location ID (used to look up coordinates from CSV)
    """
    # Prefer latitude/longitude from CSV mapping table (most reliable)
    if location_id and location_id in LOCATION_COORDS:
        sensor_lat = LOCATION_COORDS[location_id]['latitude']
        sensor_lon = LOCATION_COORDS[location_id]['longitude']

    datetime_from = parse_date_to_openaq_format(datetime_from)
    datetime_to = parse_date_to_openaq_format(datetime_to)

    records: List[dict] = []
    page = 1

    while True:
        response = client.measurements.list(
            sensors_id=sensor_id,
            datetime_from=datetime_from,
            datetime_to=datetime_to,
            data="days",
            limit=1000,
            page=page,
        )

        if not response.results:
            break

        # No need to filter for parameter type at this layer; already checked previously
        for result in response.results:
            records.append(_measurement_to_record(sensor_id, result, sensor_lat, sensor_lon))

        if len(response.results) < 1000:
            break
        page += 1

    df = pd.DataFrame(records)

    # Client-side date filtering (API may return all historical data)
    if not df.empty and datetime_from and datetime_to:
        df['datetime_utc'] = pd.to_datetime(df['datetime_utc'], utc=True)

        start_dt = pd.to_datetime(datetime_from)
        end_dt = pd.to_datetime(datetime_to)

        original_count = len(df)

        if start_dt is not None:
            df = df[df['datetime_utc'] >= start_dt]
        if end_dt is not None:
            df = df[df['datetime_utc'] <= end_dt]

        filtered_count = original_count - len(df)
        if filtered_count > 0:
            print(f"  Filtered out {filtered_count} records outside date range for sensor {sensor_id}")

    return df


def fetch_sensor_metadata(client: OpenAQ, sensor_id: int) -> Optional[dict]:
    response = client.sensors.get(sensor_id)
    if not response.results:
        return None

    sensor = response.results[0]
    lat = None
    lon = None
    if sensor.latest and sensor.latest.coordinates:
        lat = sensor.latest.coordinates.latitude
        lon = sensor.latest.coordinates.longitude

    last_seen = (
        parser.parse(sensor.datetime_last.utc)
        if sensor.datetime_last and sensor.datetime_last.utc
        else None
    )
    region = assign_region(lat, lon, sensor_id)

    return {
        'sensor_id': sensor.id,
        'sensor_name': sensor.name,
        'parameter': sensor.parameter.name if sensor.parameter else None,
        'latitude': lat,
        'longitude': lon,
        'region': region,
        'last_seen': last_seen,
    }


def assign_region(latitude: Optional[float], longitude: Optional[float], sensor_id: Optional[int] = None) -> str:
    overrides = get_region_overrides()
    if sensor_id in overrides:
        return overrides[sensor_id]

    if longitude is None:
        return 'Unknown'

    if longitude <= -105:
        return 'West'
    if longitude <= -90:
        return 'Midwest'
    return 'East'


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

def ensure_tables_exist(engine) -> None:
    sensor_data_sql = """
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
            latitude NUMERIC,
            longitude NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(sensor_id, parameter, datetime_utc)
        );
    """

    metadata_sql = """
        CREATE TABLE IF NOT EXISTS sensor_metadata (
            sensor_id INTEGER PRIMARY KEY,
            sensor_name TEXT,
            parameter VARCHAR(50),
            latitude NUMERIC,
            longitude NUMERIC,
            region VARCHAR(20),
            last_seen TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """

    signal_sql = """
        CREATE TABLE IF NOT EXISTS signal_table (
            sensor_id INTEGER PRIMARY KEY,
            last_data_datetime_utc TIMESTAMPTZ,
            last_run_time TIMESTAMPTZ DEFAULT NOW()
        );
    """

    with engine.begin() as conn:
        conn.execute(text(sensor_data_sql))
        conn.execute(text(metadata_sql))
        conn.execute(text(signal_sql))


def prepare_measurements(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    data = df.copy()
    datetime_cols = ['datetime_utc', 'datetime_local']
    for col in datetime_cols:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col])

    numeric_columns = [
        'sensor_id', 'value', 'coverage_percent', 'min', 'max', 'median',
        'latitude', 'longitude'
    ]
    for col in numeric_columns:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors='coerce')

    string_columns = ['parameter', 'units']
    for col in string_columns:
        if col in data.columns:
            data[col] = data[col].astype(str)

    return data


def save_measurements(engine, df: pd.DataFrame) -> Dict[str, int]:
    if df.empty:
        return {'attempted': 0, 'inserted': 0, 'duplicates': 0}

    cleaned = prepare_measurements(df)
    key_cols = ['sensor_id', 'parameter', 'datetime_utc']
    for col in key_cols:
        if col not in cleaned.columns:
            raise ValueError(f"Missing column {col} in measurement dataframe")

    cleaned = cleaned.drop_duplicates(subset=key_cols)
    cols = [
        'sensor_id', 'parameter', 'datetime_utc', 'datetime_local', 'value',
        'units', 'coverage_percent', 'min', 'max', 'median', 'latitude',
        'longitude'
    ]
    records = cleaned[cols].to_dict(orient='records')

    insert_sql = text(
        """
        INSERT INTO sensor_data (
            sensor_id, parameter, datetime_utc, datetime_local, value,
            units, coverage_percent, min, max, median, latitude, longitude
        ) VALUES (
            :sensor_id, :parameter, :datetime_utc, :datetime_local, :value,
            :units, :coverage_percent, :min, :max, :median, :latitude, :longitude
        )
        ON CONFLICT (sensor_id, parameter, datetime_utc) DO NOTHING
        """
    )

    with engine.begin() as conn:
        result = conn.execute(insert_sql, records)
        inserted = result.rowcount

    attempted = len(records)
    return {
        'attempted': attempted,
        'inserted': inserted,
        'duplicates': attempted - inserted,
    }


def upsert_sensor_metadata(engine, metadata_rows: List[dict]) -> None:
    if not metadata_rows:
        return

    sql = text(
        """
        INSERT INTO sensor_metadata (
            sensor_id, sensor_name, parameter, latitude, longitude, region,
            last_seen, updated_at
        ) VALUES (
            :sensor_id, :sensor_name, :parameter, :latitude, :longitude,
            :region, :last_seen, NOW()
        )
        ON CONFLICT (sensor_id) DO UPDATE SET
            sensor_name = EXCLUDED.sensor_name,
            parameter = EXCLUDED.parameter,
            latitude = COALESCE(EXCLUDED.latitude, sensor_metadata.latitude),
            longitude = COALESCE(EXCLUDED.longitude, sensor_metadata.longitude),
            region = EXCLUDED.region,
            last_seen = COALESCE(EXCLUDED.last_seen, sensor_metadata.last_seen),
            updated_at = NOW()
        """
    )

    with engine.begin() as conn:
        conn.execute(sql, metadata_rows)


def get_signal_windows(engine, sensor_ids: List[int]) -> Dict[int, str]:
    default_start = get_default_start_date()
    windows = {sensor_id: default_start for sensor_id in sensor_ids}

    if not sensor_ids:
        return windows

    query = text("SELECT sensor_id, last_data_datetime_utc FROM signal_table")
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    valid_ids = set(sensor_ids)
    for sensor_id, last_dt in rows:
        if sensor_id in valid_ids and last_dt:
            windows[sensor_id] = parse_date_to_openaq_format(last_dt)
    return windows


def update_signal_table(engine, latest_per_sensor: Dict[int, str]) -> None:
    if not latest_per_sensor:
        return

    sql = text(
        """
        INSERT INTO signal_table (sensor_id, last_data_datetime_utc, last_run_time)
        VALUES (:sensor_id, :last_dt, NOW())
        ON CONFLICT (sensor_id) DO UPDATE SET
            last_data_datetime_utc = EXCLUDED.last_data_datetime_utc,
            last_run_time = NOW()
        """
    )

    payload = [
        {
            'sensor_id': sensor_id,
            'last_dt': parser.parse(last_dt) if isinstance(last_dt, str) else last_dt,
        }
        for sensor_id, last_dt in latest_per_sensor.items()
    ]

    with engine.begin() as conn:
        conn.execute(sql, payload)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
