from openaq import OpenAQ
import pandas as pd
import os
from dotenv import load_dotenv
from dateutil import parser 
from datetime import datetime
from sqlalchemy import create_engine, text
from pathlib import Path


# Load environment variables
load_dotenv()

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


def get_sensor_data(client, sensor_id, datetime_from="2025-11-13T00:00:00Z", datetime_to="2025-11-13T00:00:00Z"):
    """
    Fetch data for a single sensor from OpenAQ API.
    
    Parameters:
    -----------
    client : OpenAQ
        OpenAQ client instance with API key
    sensor_id : int
        Sensor ID to fetch data for
    datetime_from : str
        Start date in various formats (will be parsed)
    datetime_to : str
        End date in various formats (will be parsed)
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with columns: sensor_id, parameter, datetime_utc, datetime_local,
        value, units, coverage_percent, min, max, median
    """
    datetime_from = parse_date_to_openaq_format(datetime_from)
    datetime_to = parse_date_to_openaq_format(datetime_to)

    all_data = []
    page = 1
    
    print(f"\nFetching data for sensor {sensor_id}...")
    
    while True:
        try:
            response = client.measurements.list(
                sensors_id=sensor_id,
                datetime_from=datetime_from,  
                datetime_to=datetime_to,      
                data="days",
                limit=1000,
                page=page
            )
            
            if not response.results:
                break
            
            for result in response.results:
                all_data.append({
                    'sensor_id': sensor_id,
                    'parameter': result.parameter.name,
                    'datetime_utc': result.period.datetime_from.utc,
                    'datetime_local': result.period.datetime_from.local,
                    'value': result.value,
                    'units': result.parameter.units,
                    'coverage_percent': result.coverage.percent_complete if result.coverage else None,
                    'min': result.summary.min if result.summary else None,
                    'max': result.summary.max if result.summary else None,
                    'median': result.summary.median if result.summary else None,
                })
            
            print(f"  Page {page}: {len(response.results)} records")
            
            if len(response.results) < 1000:
                break
            
            page += 1
            
        except Exception as e:
            print(f"  Error on page {page}: {e}")
            break
    
    df = pd.DataFrame(all_data)
    print(f"Collected {len(df)} records for sensor {sensor_id}")
    
    return df


def prepare_dataframe_for_db(df):
    """
    Prepare the DataFrame for database insertion by ensuring correct data types.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Raw DataFrame from API
    
    Returns:
    --------
    pd.DataFrame
        Cleaned DataFrame ready for database insertion
    """
    if df.empty:
        return df
    
    df_clean = df.copy()
    
    # Convert datetime columns to proper datetime format
    if 'datetime_utc' in df_clean.columns:
        df_clean['datetime_utc'] = pd.to_datetime(df_clean['datetime_utc'])
    
    if 'datetime_local' in df_clean.columns:
        df_clean['datetime_local'] = pd.to_datetime(df_clean['datetime_local'])
    
    # Ensure numeric columns are proper numeric types
    numeric_columns = ['sensor_id', 'value', 'coverage_percent', 'min', 'max', 'median']
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Ensure string columns are strings
    string_columns = ['parameter', 'units']
    for col in string_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
    
    return df_clean


def save_dataframe_to_db(df, table_name='sensor_data', if_exists='append'):
    """
    Save a pandas DataFrame to PostgreSQL database.
    Uses batch insert with ON CONFLICT to handle duplicates efficiently.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame to save
    table_name : str
        Name of the table to save to (default: 'sensor_data')
    if_exists : str
        How to behave if table exists (default: 'append')
    
    Returns:
    --------
    dict
        Summary with 'attempted', 'status' counts
    """
    if df.empty:
        print("⚠ DataFrame is empty, nothing to insert")
        return {'attempted': 0, 'status': 'empty'}
    
    # Prepare the dataframe
    df_clean = prepare_dataframe_for_db(df)
    
    engine = get_db_engine()
    
    try:
        # Count existing records before insert
        from sqlalchemy import text
        with engine.connect() as conn:
            count_before = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]
        
        # Use pandas to_sql with special handling
        # This will fail on duplicates, but we'll catch it
        try:
            df_clean.to_sql(
                table_name,
                engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            # Count after insert
            with engine.connect() as conn:
                count_after = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]
            
            inserted = count_after - count_before
            print(f"✓ Inserted {inserted} rows into {table_name}")
            return {'attempted': len(df_clean), 'inserted': inserted, 'duplicates': len(df_clean) - inserted}
            
        except Exception as e:
            if 'duplicate key' in str(e).lower() or 'unique constraint' in str(e).lower():
                print(f"⚠ Some duplicates detected, using conflict resolution...")
                
                # Use raw SQL with ON CONFLICT
                with engine.begin() as conn:
                    # Get count before
                    count_before = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]
                    
                    # Prepare values for batch insert
                    values = []
                    for _, row in df_clean.iterrows():
                        values.append({
                            'sensor_id': int(row['sensor_id']),
                            'parameter': str(row['parameter']),
                            'datetime_utc': row['datetime_utc'],
                            'datetime_local': row['datetime_local'],
                            'value': float(row['value']) if pd.notna(row['value']) else None,
                            'units': str(row['units']),
                            'coverage_percent': float(row['coverage_percent']) if pd.notna(row['coverage_percent']) else None,
                            'min': float(row['min']) if pd.notna(row['min']) else None,
                            'max': float(row['max']) if pd.notna(row['max']) else None,
                            'median': float(row['median']) if pd.notna(row['median']) else None
                        })
                    
                    # Batch insert with ON CONFLICT
                    insert_stmt = text("""
                        INSERT INTO sensor_data 
                        (sensor_id, parameter, datetime_utc, datetime_local, value, units, 
                         coverage_percent, min, max, median)
                        VALUES 
                        (:sensor_id, :parameter, :datetime_utc, :datetime_local, :value, :units,
                         :coverage_percent, :min, :max, :median)
                        ON CONFLICT (sensor_id, parameter, datetime_utc) DO NOTHING
                    """)
                    
                    conn.execute(insert_stmt, values)
                    
                    # Get count after
                    count_after = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]
                
                inserted = count_after - count_before
                duplicates = len(df_clean) - inserted
                print(f"✓ Inserted {inserted} new rows, skipped {duplicates} duplicates")
                return {'attempted': len(df_clean), 'inserted': inserted, 'duplicates': duplicates}
            else:
                raise
                
    except Exception as e:
        print(f"✗ Error saving to database: {e}")
        raise
    finally:
        engine.dispose()
    


def fetch_and_save_sensor_data(client, sensor_ids, datetime_from, datetime_to, table_name='sensor_data'):
    """
    Fetch data for multiple sensors from OpenAQ API and save to PostgreSQL database.
    
    Parameters:
    -----------
    client : OpenAQ
        OpenAQ client instance with API key
    sensor_ids : list
        List of sensor IDs to fetch data for
    datetime_from : str
        Start date in format 'MM/DD/YYYY' or other parseable formats
    datetime_to : str
        End date in format 'MM/DD/YYYY' or other parseable formats
    table_name : str
        Name of database table to save to (default: 'sensor_data')
    
    Returns:
    --------
    dict
        Summary with 'successful', 'failed', 'total', and 'total_rows' counts
    """
    successful = 0
    failed = 0
    total_rows = 0
    
    for idx, sensor_id in enumerate(sensor_ids, 1):
        print(f"\n[{idx}/{len(sensor_ids)}] Processing sensor {sensor_id}")
        print("=" * 70)
        
        try:
            # Fetch data from API
            df = get_sensor_data(client, sensor_id, datetime_from=datetime_from, datetime_to=datetime_to)
            
            if not df.empty:
                # Save to database
                rows_inserted = save_dataframe_to_db(df, table_name=table_name, if_exists='append')
                total_rows += rows_inserted
                successful += 1
            else:
                print(f"⚠ No data for sensor {sensor_id}")
                failed += 1
                
        except Exception as e:
            print(f"✗ Error processing sensor {sensor_id}: {e}")
            failed += 1
    
    # Print summary

def fetch_and_save_sensor_data(client, sensor_ids, datetime_from, datetime_to, table_name='sensor_data'):
    """
    Fetch data for multiple sensors from OpenAQ API and save to PostgreSQL database.
    """
    successful = 0
    failed = 0
    total_rows_attempted = 0
    total_rows_inserted = 0
    total_duplicates = 0
    
    for idx, sensor_id in enumerate(sensor_ids, 1):
        print(f"\n[{idx}/{len(sensor_ids)}] Processing sensor {sensor_id}")
        print("=" * 70)
        
        try:
            # Fetch data from API
            df = get_sensor_data(client, sensor_id, datetime_from=datetime_from, datetime_to=datetime_to)
            
            if not df.empty:
                # Save to database
                result = save_dataframe_to_db(df, table_name=table_name, if_exists='append')
                
                total_rows_attempted += result.get('attempted', 0)
                total_rows_inserted += result.get('inserted', 0)
                total_duplicates += result.get('duplicates', 0)
                successful += 1
            else:
                print(f"⚠ No data for sensor {sensor_id}")
                failed += 1
                
        except Exception as e:
            print(f"✗ Error processing sensor {sensor_id}: {e}")
            failed += 1
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Successful sensors: {successful}")
    print(f"Failed sensors: {failed}")
    print(f"Total sensors: {len(sensor_ids)}")
    print(f"Rows attempted: {total_rows_attempted}")
    print(f"Rows inserted: {total_rows_inserted}")
    print(f"Duplicates skipped: {total_duplicates}")
    
    return {
        'successful': successful,
        'failed': failed,
        'total': len(sensor_ids),
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