from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
from modules.aq_utils import get_sensor_data
import pandas as pd
import os

# ==========  BUILD SQLALCHEMY ENGINE ==========
def get_engine():
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    pw = os.getenv("POSTGRES_PASSWORD")

    if not all([host, port, db, user, pw]):
        raise Exception("Missing PostgreSQL environment variables!")

    url = (
        f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
        f"?sslmode=require"
    )

    engine = create_engine(url)
    return engine


# ==========  TASK 1: TEST DB CONNECTION ==========
def test_postgres_connection():
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT NOW();"))
            print("Connection SUCCESS:", list(result))
    except Exception as e:
        raise Exception(f"Connection FAILED: {e}")


# ==========  TASK 2: CREATE TABLES ==========
def create_tables():
    engine = get_engine()

    raw_data_sql = """
    CREATE TABLE IF NOT EXISTS raw_data (
        id BIGSERIAL PRIMARY KEY,
        sensor_id INT,
        parameter TEXT,
        datetime_utc TIMESTAMPTZ,
        datetime_local TIMESTAMPTZ,
        value DOUBLE PRECISION,
        units TEXT,
        coverage_percent DOUBLE PRECISION,
        min DOUBLE PRECISION,
        max DOUBLE PRECISION,
        median DOUBLE PRECISION
    );
    """

    signal_sql = """
    CREATE TABLE IF NOT EXISTS signal_table (
        sensor_id INT PRIMARY KEY,
        last_data_datetime_utc TIMESTAMPTZ,
        last_run_time TIMESTAMPTZ DEFAULT NOW()
    );
    """

    with engine.begin() as conn:
        conn.execute(text(raw_data_sql))
        conn.execute(text(signal_sql))

    print("Tables created successfully")

# ==========  TASK 3: READ SIGNAL TABLE  ==========
def read_signal(**context):
    engine = get_engine()
    sensor_id = 1  # 暂时固定，之后我们扩展

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT last_data_datetime_utc FROM signal_table WHERE sensor_id = :sid"),
            {"sid": sensor_id}
        ).fetchone()

    if row:
        datetime_from = row[0].strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"Found signal → last datetime = {datetime_from}")
    else:
        datetime_from = "2020-01-01T00:00:00Z"
        print("No signal found → using default start date.")

    # Push to XCom
    context["ti"].xcom_push(key="sensor_id", value=sensor_id)
    context["ti"].xcom_push(key="datetime_from", value=datetime_from)

# ==========  TASK 4: FETCH DATA USING FUNCTION  ==========
def fetch_sensor_data(**context):
    sensor_id = context["ti"].xcom_pull(key="sensor_id")
    datetime_from = context["ti"].xcom_pull(key="datetime_from")
    datetime_to = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    print("Calling get_sensor_data()...")
    df = get_sensor_data(
        client=None,
        sensor_id=sensor_id,
        datetime_from=datetime_from,
        datetime_to=datetime_to,
    )

    print(f"Fetched {len(df)} rows")

    # Convert DataFrame → JSON for XCom
    context["ti"].xcom_push(key="fetched_data", value=df.to_dict(orient="records"))

# ==========  TASK 5: WRITE RAW_DATA TABLE  ==========
def write_raw_data(**context):
    engine = get_engine()
    rows = context["ti"].xcom_pull(key="fetched_data")

    if not rows:
        print("No new data → skipping write.")
        return

    df = pd.DataFrame(rows)
    df.to_sql("raw_data", engine, if_exists="append", index=False)

    print(f"Wrote {len(df)} rows to raw_data")

# ==========  TASK 6: UPDATE SIGNAL TABLE  ==========
def update_signal(**context):
    engine = get_engine()
    rows = context["ti"].xcom_pull(key="fetched_data")
    sensor_id = context["ti"].xcom_pull(key="sensor_id")

    if not rows:
        print("No rows → signal not updated")
        return

    df = pd.DataFrame(rows)
    latest_dt = df["datetime_utc"].max()

    sql = """
        INSERT INTO signal_table(sensor_id, last_data_datetime_utc, last_run_time)
        VALUES (:sid, :ldt, NOW())
        ON CONFLICT (sensor_id)
        DO UPDATE SET
            last_data_datetime_utc = EXCLUDED.last_data_datetime_utc,
            last_run_time = NOW();
    """

    with engine.begin() as conn:
        conn.execute(text(sql), {"sid": sensor_id, "ldt": latest_dt})

    print(f"Signal table updated → {latest_dt}")

# ==========  DAG DEFINITION ==========
with DAG(
    dag_id="supabase_init_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # ----- TASK 1: DB Connection Test -----
    test_conn_task = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_connection,
    )

    # ----- TASK 2: Create Tables -----
    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    # ----- TASK 3: Read Signal -----
    read_signal_task = PythonOperator(
        task_id="read_signal",
        python_callable=read_signal,
        provide_context=True,
    )

    # ----- TASK 4: Fetch Data -----
    fetch_data_task = PythonOperator(
        task_id="fetch_sensor_data",
        python_callable=fetch_sensor_data,
        provide_context=True,
    )

    # ----- TASK 5: Write Raw Data -----
    write_raw_task = PythonOperator(
        task_id="write_raw_data",
        python_callable=write_raw_data,
        provide_context=True,
    )

    # ----- TASK 6: Update Signal -----
    update_signal_task = PythonOperator(
        task_id="update_signal_table",
        python_callable=update_signal,
        provide_context=True,
    )

    # FULL PIPELINE ORDER
    test_conn_task >> create_tables_task >> read_signal_task >> fetch_data_task >> write_raw_task >> update_signal_task