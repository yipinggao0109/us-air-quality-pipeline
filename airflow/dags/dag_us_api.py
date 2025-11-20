from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import text
import pandas as pd

from modules.aq_utils import (
    ensure_tables_exist,
    fetch_sensor_measurements,
    get_engine,
    get_openaq_client,
    get_location_ids,
    get_default_start_date,
    get_pm25_sensors_from_location,
    save_measurements,
    utcnow_iso,
)


# ==========  TASK 1: TEST DB CONNECTION ==========
def test_postgres_connection():
    """
    Test database connection

    What this function does:
    - test = to test
    - postgres_connection = PostgreSQL database connection

    Description:
    1. Try connecting to the database
    2. Run SELECT NOW() to query the current time
    3. Print success message if connection works
    4. If it fails, raises an error and Airflow will mark the task as failed
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT NOW()"))
        print("Connection SUCCESS:", result.fetchone()[0])


# ==========  TASK 2: CREATE TABLES ==========
def create_tables():
    """
    Create required tables

    What this function does:
    - create = to create
    - tables = database tables

    Description:
    1. Ensure sensor_data (measurement data) table exists
    2. If it already exists, will not create again (uses CREATE TABLE IF NOT EXISTS)
    """
    engine = get_engine()
    ensure_tables_exist(engine)
    print("Ensured sensor_data table exists")


# ==========  TASK 3: PREPARE SENSOR LIST  ==========
def prepare_location_list(**context):
    """
    Prepare the location list to process (location-based approach)

    ✨ Using the location-based approach as in fetch_data_to_csv.py:
    1. Read location IDs from states_codes.csv (50 states)
    2. Use default start date (2024-01-01)
    3. Pass info to the next task via XCom

    Note: No longer filter sensors here — PM2.5 sensors are filtered in the next task for each location.
    """
    all_location_ids = get_location_ids()
    default_start = get_default_start_date()

    # Store location IDs and start date in XCom
    context["ti"].xcom_push(key="location_ids", value=all_location_ids)
    context["ti"].xcom_push(key="default_start", value=default_start)

    print(f"\n📊 Summary:")
    print(f"  Total locations: {len(all_location_ids)}")
    print(f"  Start date: {default_start}")
    print(f"  Will filter PM2.5 sensors from each location")


# ==========  TASK 4: FETCH DATA USING OPENAQ (LOCATION-BASED) ==========
def fetch_location_data(**context):
    """
    Fetch location data from the OpenAQ API (location-based method)

    ✨ Follows the method in fetch_data_to_csv.py:
    1. Iterate through each location
    2. Get all sensors from the location
    3. Filter PM2.5 sensors
    4. Fetch measurements from these PM2.5 sensors

    Description:
    1. Read location IDs passed from the previous task through XCom
    2. For each location, get PM2.5 sensors
    3. For each PM2.5 sensor, fetch measurement data
    4. Pass all collected data to the next task through XCom
    """
    # Read output from previous task via XCom
    location_ids = context["ti"].xcom_pull(key="location_ids") or []
    datetime_from = context["ti"].xcom_pull(key="default_start")
    datetime_to = utcnow_iso()  # Up to now

    client = get_openaq_client()
    measurement_rows = []  # Collected measurement data

    print(f"Fetching data for {len(location_ids)} locations...")

    for idx, location_id in enumerate(location_ids, start=1):
        print(f"\n[{idx}/{len(location_ids)}] Processing location {location_id}")
        
        try:
            # Get PM2.5 sensors from the location
            pm25_sensors = get_pm25_sensors_from_location(client, location_id)
            
            if not pm25_sensors:
                print(f"  No PM2.5 sensors found at location {location_id}")
                continue
            
            # For each PM2.5 sensor, fetch measurements
            for sensor in pm25_sensors:
                try:
                    # Get coordinates from sensor object (fallback)
                    sensor_coords = getattr(sensor, 'coordinates', None)
                    sensor_lat = getattr(sensor_coords, 'latitude', None) if sensor_coords else None
                    sensor_lon = getattr(sensor_coords, 'longitude', None) if sensor_coords else None
                    
                    print(f"  Fetching sensor {sensor.id} ({sensor.name})...")
                    df = fetch_sensor_measurements(
                        client=client,
                        sensor_id=sensor.id,
                        datetime_from=datetime_from,
                        datetime_to=datetime_to,
                        sensor_lat=sensor_lat,
                        sensor_lon=sensor_lon,
                        location_id=location_id,  # Pass location_id, used for lookup if needed
                    )
                    
                    if not df.empty:
                        measurement_rows.extend(df.to_dict(orient="records"))
                        print(f"  ✓ Fetched {len(df)} rows for sensor {sensor.id}")
                    else:
                        print(f"  ⚠️  No data for sensor {sensor.id}")
                        
                except Exception as exc:
                    print(f"  ✗ Failed to fetch sensor {sensor.id}: {exc}")
                    continue
                    
        except Exception as exc:
            print(f"✗ Failed to process location {location_id}: {exc}")
            continue  # Continue with next location
        else:
            print(f"⚠ No new rows for sensor {sensor_id}")

    # Save results to XCom for the next task
    context["ti"].xcom_push(key="fetched_data", value=measurement_rows)

    print(
        f"Aggregated {len(measurement_rows)} PM2.5 measurement rows across {len(sensor_ids)} sensors"
    )


# ==========  TASK 5: WRITE SENSOR_DATA TABLE  ==========
def write_sensor_data(**context):
    """
    Write fetched data to database

    What this function does:
    - write = to write
    - sensor_data = sensor data

    Description:
    1. Read data fetched by previous task from XCom
    2. Write PM2.5 measurement data to sensor_data table
    3. Use ON CONFLICT DO NOTHING to avoid duplicates
    4. Print summary statistics (inserts, duplicates skipped)
    """
    engine = get_engine()
    # Read data from XCom
    rows = context["ti"].xcom_pull(key="fetched_data") or []

    if rows:
        # Convert list of dicts to DataFrame
        df = pd.DataFrame(rows)
        # Save measurement data into sensor_data table
        stats = save_measurements(engine, df)
        print(
            "sensor_data insert summary:",
            f"attempted={stats['attempted']}",
            f"inserted={stats['inserted']}",
            f"duplicates={stats['duplicates']}",
        )
    else:
        print("No measurement rows to persist")




# ==========  DAG DEFINITION ==========
# DAG (Directed Acyclic Graph) is the core concept in Airflow.
# It defines a set of tasks that have upstream and downstream dependencies.
with DAG(
    dag_id="supabase_init_pipeline",        # Unique ID for this DAG
    start_date=datetime(2024, 1, 1),        # Start date of the DAG
    schedule_interval="@daily",             # Run every day
    catchup=False,                          # False = do not backfill past dates
) as dag:

    # Task 1: Test database connection
    test_conn_task = PythonOperator(
        task_id="test_postgres_connection",     # Unique ID for this task
        python_callable=test_postgres_connection, # Function to run
    )

    # Task 2: Create database tables
    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    # Task 3: Prepare location list (location-based approach)
    prepare_list_task = PythonOperator(
        task_id="prepare_location_list",
        python_callable=prepare_location_list,
        provide_context=True,  # Allow the function to receive context (for XCom)
    )

    # Task 4: Fetch data from OpenAQ API (location-based approach)
    fetch_data_task = PythonOperator(
        task_id="fetch_location_data",
        python_callable=fetch_location_data,
        provide_context=True,
    )

    # Task 5: Write data to database
    write_sensor_data_task = PythonOperator(
        task_id="write_sensor_data",
        python_callable=write_sensor_data,
        provide_context=True,
    )

    # Define the execution sequence for the tasks.
    # The >> operator means "then" (A >> B means run B after A).
    # This location-based pipeline order:
    # 1. Test connection
    # 2. Create tables
    # 3. Prepare location list (50 states)
    # 4. Fetch data from PM2.5 sensors at each location
    # 5. Write to database
    test_conn_task >> create_tables_task >> prepare_list_task >> fetch_data_task >> write_sensor_data_task
