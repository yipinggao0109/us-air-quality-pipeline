# US Air Quality Pipeline

End-to-end data pipeline that fetches PM2.5 air quality data from OpenAQ API, stores it in PostgreSQL (Supabase), and visualizes it through an interactive Streamlit dashboard.

PM2.5 refers to **fine particulate matter**—tiny particles with diameter ≤ 2.5 micrometers that can penetrate deep into the lungs and bloodstream and are strongly linked to cardiovascular and respiratory problems. We focus on PM2.5 (instead of ozone or other pollutants) because it has well-documented health impacts, clear regulatory thresholds, and strong spatial variation across regions, making it a natural choice for a first version of this monitoring pipeline.


## 🏗️ Architecture

```
┌─────────────────┐
│   OpenAQ API    │  ← Real-time air quality data
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│  Apache Airflow │  ← Daily automated data fetching
│   (Scheduler)   │     • Location-based fetching
└────────┬────────┘     • PM2.5 filtering
         │              • Incremental updates
         ↓
┌─────────────────┐
│   PostgreSQL    │  ← Central data storage
│   (Supabase)    │     • sensor_data table
└────────┬────────┘     • Coordinates from CSV
         │
         ↓
┌─────────────────┐
│   Streamlit     │  ← Interactive dashboard
│   Dashboard     │     • Map visualization
└─────────────────┘     • Filters & metrics
```

### Key Components

1. **Data Fetching**: Two methods for data ingestion
   - **Manual**: `src/fetch_data/main.py` - One-time historical data fetch
   - **Automated**: Airflow DAG - Daily incremental updates

2. **Data Storage**: PostgreSQL database with single table
   - `sensor_data`: PM2.5 measurements with coordinates

3. **Visualization**: Streamlit web application
   - Interactive map with color-coded PM2.5 levels
   - Date range and location filters
   - Key metrics and monitoring site details

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- OpenAQ API Key (free from https://openaq.org)
- Supabase account (free tier works)

### Step 1: Clone Repository

```bash
git clone https://github.com/yipinggao0109/us-air-quality-pipeline.git
cd us-air-quality-pipeline
```

### Step 2: Set Up Supabase Database

1. **Create a Supabase project**
   - Go to https://supabase.com
   - Register a new account
   - Click "New Project"
   - Choose a name, database password, and region
   - Wait for setup to complete (~2 minutes)

2. **Get connection details**
   - In your project dashboard, go to `Connect` → `Connection String` → `Method` (Choose `Session Pooler`)
   - Note down:
     - **Host**: `db.xxxxxxxxxxxxx.supabase.com` # example: aws-0-us-west-2.pooler.supabase.com
     - **Port**: `5432`
     - **Database name**: `postgres`
     - **User**: `postgres`
     - **Password**: [Your database password] # the password you choose on supabase

3. **Create the database table**
   - Go to `SQL Editor` in Supabase dashboard
   - Click "New Query"
   - Copy and paste the contents of `init_db/01_create_tables.sql`
   - Click "Run" or press `Ctrl+Enter`
   - You should see: "Success. No rows returned"

### Step 3: Configure Environment Variables

Create a `.env` file in the project root:

```bash
# OpenAQ API Configuration
OPENAQ_API_KEY=your_openaq_api_key_here
OPENAQ_DEFAULT_START_DATE=2024-01-01T00:00:00Z

# Supabase/PostgreSQL Configuration
POSTGRES_HOST=db.xxxxxxxxxxxxx.supabase.com
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_database_password_here
POSTGRES_SSLMODE=require

# PgAdmin configuration
PGADMIN_EMAIL=admin@airquality.com
PGADMIN_PASSWORD=admin
```

**How to get OpenAQ API Key:**
1. Go to https://openaq.org
2. Sign up for a free account
3. Navigate to your profile/API settings
4. Copy your API key

### Step 4: Set Up Python Virtual Environment (Optional but Recommended)

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 5: Initial Data Fetch (Manual Method)

Before starting Docker services, fetch some initial data:

```bash
# Make sure virtual environment is activated
source venv/bin/activate

# Run manual data fetch
python src/fetch_data/main.py
```

This will:
- Fetch PM2.5 data for 50 US states (from 2024-01-01 to today)
- Filter for PM2.5 sensors only
- Store data in your Supabase database
- **Takes 10-30 minutes** depending on data volume

**Expected output:**
```
✅ Successfully import 50 location_ids from states_codes.csv 
[1/50] Location 1671
  ↳ Found 5 PM2.5 sensors at location 1671
  Fetching sensor 12345...
  ✓ Fetched 365 rows for PM2.5 sensor 12345
...
Total rows inserted: 45,230
```

### Step 6: Start Docker Services

```bash
# Start all services
docker compose up -d

# Check status
docker compose ps
```

You should see:
- `airflow_scheduler` - Running
- `airflow_webserver` - Running
- `streamlit_frontend` - Running
- `pgadmin_airquality` - Running (optional)

### Step 7: Access the Applications

#### Streamlit Dashboard (Main UI)
- **URL**: http://localhost:8501
- **Features**:
  - Interactive US map with PM2.5 data points
  - Color-coded by air quality (green = good, red = unhealthy)
  - Date range filter
  - Location filter
  - Key metrics display

#### Airflow Web UI (Data Pipeline Management)
- **URL**: http://localhost:8080
- **Login**: 
  - Username: `admin`
  - Password: `admin`
- **To trigger manual DAG run**:
  1. Find `supabase_init_pipeline` in the DAG list
  2. Toggle it to "Active" (switch on the right)
  3. Click the "Play" button → "Trigger DAG"

#### pgAdmin (Database Management) - Optional
- **URL**: http://localhost:5050
- **Login**:
  - Email: `admin@airquality.com`
  - Password: `admin`

## 📊 Data Pipeline Details

### Data Sources

The pipeline fetches data from **50 US state locations** defined in:
- `src/frontend/sensor_locations/states_codes.csv` - Location IDs for each state
- `src/frontend/sensor_locations/openaq_us_locations_enriched.csv` - Location names and coordinates

### Data Fetching Strategy

**Location-Based Approach:**
1. For each state location ID
2. Get all sensors at that location
3. Filter for PM2.5 sensors only
4. Fetch daily measurements
5. Attach coordinates from CSV (for map display)

### Database Schema

**`sensor_data` table:**
```sql
CREATE TABLE sensor_data (
    id SERIAL PRIMARY KEY,
    sensor_id INTEGER NOT NULL,
    parameter VARCHAR(50) NOT NULL,           -- Always 'pm25'
    datetime_utc TIMESTAMP NOT NULL,
    datetime_local TIMESTAMP NOT NULL,
    value NUMERIC,                            -- PM2.5 concentration (µg/m³)
    units VARCHAR(20),
    coverage_percent NUMERIC,
    min NUMERIC,
    max NUMERIC,
    median NUMERIC,
    latitude NUMERIC,                         -- From CSV
    longitude NUMERIC,                        -- From CSV
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sensor_id, parameter, datetime_utc)  -- Prevents duplicates
);
```

### Airflow DAG Schedule

- **DAG Name**: `supabase_init_pipeline`
- **Schedule**: Daily at midnight UTC
- **Catchup**: Disabled (won't backfill missed runs)
- **Tasks**:
  1. `test_postgres_connection` - Verify database connectivity
  2. `create_tables` - Ensure table exists
  3. `prepare_location_list` - Load 50 state location IDs
  4. `fetch_location_data` - Fetch PM2.5 data from OpenAQ
  5. `write_sensor_data` - Insert data into PostgreSQL

## 🔧 Maintenance & Troubleshooting

### View Logs

```bash
# Airflow scheduler logs
docker logs airflow_scheduler

# Airflow webserver logs
docker logs airflow_webserver

# Streamlit logs
docker logs streamlit_frontend
```

### Clear and Restart

If you need to start fresh:

```bash
# Stop all services
docker compose down

# Remove volumes (⚠️ deletes all data)
docker compose down -v

# Restart
docker compose up -d
```

### Clear Database Only

If you want to re-fetch all data:

```bash
# Activate virtual environment
source venv/bin/activate

# Run clear script
python clear_database.py

# Re-fetch data
python src/fetch_data/main.py
```

### Common Issues

**Issue**: "No PM2.5 measurements available in PostgreSQL"
- **Solution**: Run `python src/fetch_data/main.py` to fetch initial data

**Issue**: Airflow DAG not appearing
- **Solution**: Check logs `docker logs airflow_scheduler` for import errors

**Issue**: Map shows no data points
- **Solution**: Verify coordinates exist in database:
  ```sql
  SELECT COUNT(*) FROM sensor_data WHERE latitude IS NOT NULL;
  ```

**Issue**: Docker containers keep restarting
- **Solution**: Check if ports 8080, 8501, 5050 are already in use

## 📁 Project Structure

```
us-air-quality-pipeline/
├── airflow/                          # Airflow DAGs and modules
│   └── dags/
│       ├── dag_us_api.py            # Main Airflow DAG
│       └── modules/
│           └── aq_utils.py          # Data fetching utilities
├── src/
│   ├── fetch_data/                  # Manual data fetching
│   │   ├── main.py                  # Entry point
│   │   └── fetch_data_modules/
│   │       └── fetch_data_to_db.py  # Core fetching logic
│   └── frontend/                    # Streamlit dashboard
│       ├── app.py                   # Main Streamlit app
│       ├── utils/
│       │   └── db_loader.py         # Database queries
│       └── sensor_locations/
│           ├── states_codes.csv     # 50 US state location IDs
│           └── openaq_us_locations_enriched.csv  # Location names & coordinates
├── init_db/
│   └── 01_create_tables.sql        # Database schema
├── docker-compose.yaml              # Docker services configuration
├── requirements.txt                 # Python dependencies
├── .env                            # Environment variables (create this)
└── README.md                       # This file
```

## 🎨 Dashboard Features

### Map Visualization
- **Color Coding** (AQI Standard):
  - 🟢 Green (0-12 µg/m³): Good
  - 🟡 Yellow (12-35 µg/m³): Moderate
  - 🟠 Orange (35-55 µg/m³): Unhealthy for Sensitive Groups
  - 🔴 Red (55-150 µg/m³): Unhealthy
  - 🟣 Purple (150+ µg/m³): Very Unhealthy

### Filters
- **Scenario**: Pre-defined PM2.5 threshold filters
- **Date Range**: Select start and end dates
- **Locations**: Multi-select specific monitoring sites

### Key Metrics
- **Locations**: Number of unique monitoring sites
- **Average PM2.5**: Mean concentration across filters
- **Peak PM2.5**: Highest recorded value
- **Rows**: Total measurements in view

## 🔄 Data Update Frequency

- **Airflow DAG**: Runs daily at midnight UTC
- **Manual Fetch**: Run anytime with `python src/fetch_data/main.py`
- **Dashboard**: Updates in real-time from database

## 📝 Notes

- The pipeline only fetches **PM2.5** data (not PM10 or other parameters)
- Duplicate data is automatically prevented by database unique constraint
- Date filtering happens at data fetch time for efficiency

