"""
Main script for manual data fetching

Purpose of this file:
- Used for manual, one-time bulk historical data fetches
- Complements Airflow's automated daily incremental fetching (Airflow handles daily updates)
- Both manual and automated methods use the same database table and data format

When to use:
1. When initializing the system for the first time to fetch historical data
2. When the system is interrupted and requires backfilling data
3. When you need to re-fetch data for a specific date range

How to run:
python src/fetch_data/main.py
"""

from openaq import OpenAQ
import pandas as pd
import os
from dotenv import load_dotenv
from dateutil import parser 
from datetime import datetime
from fetch_data_modules import *

# Load the API key from the project's root .env file
# load_dotenv() automatically finds the .env file and loads environment variables from it
load_dotenv()
api_key = os.getenv("OPENAQ_API_KEY")
client = OpenAQ(api_key=api_key)

# Make sure the output directory exists (if you want to save CSV files)
os.makedirs('fetched_data/sensor_data', exist_ok=True)

# Read location IDs from states_codes.csv
# These are representative monitoring station locations for the 50 US states
# Each location may contain multiple sensors; we will filter for PM2.5 sensors
import csv

csv_path = os.path.join(os.path.dirname(__file__), '../frontend/sensor_locations/states_codes.csv')

location_ids = []
try:
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('Code'):  # Ensure the Code field is not empty
                try:
                    location_ids.append(int(row['Code']))
                except ValueError:
                    continue  # skip invalid values

    print(f"✅ Successfully loaded {len(location_ids)} location IDs from states_codes.csv")
    print(f"   Range: Alabama (Location {min(location_ids)}) to Wyoming (Location {max(location_ids)})")
    print(f"   Will filter PM2.5 sensors from each location")
except FileNotFoundError:
    print(f"⚠️  Could not find {csv_path}")
    print("Using the default 50 state location IDs")
    # Use all 50 state location IDs as predefined below
    location_ids = [
        1671,  # Alabama
        1404,  # Alaska
        564,   # Arizona
        2045,  # Arkansas
        8830,  # California
        2183,  # Colorado
        2151,  # Connecticut
        1149,  # Delaware
        1560,  # Florida
        1951,  # Georgia
        2090,  # Hawaii
        1398,  # Idaho
        8864,  # Illinois
        314,   # Indiana
        1619,  # Iowa
        1155,  # Kansas
        1897,  # Kentucky
        1754,  # Louisiana
        1448,  # Maine
        1142,  # Maryland
        452,   # Massachusetts
        7003,  # Michigan
        1772,  # Minnesota
        326,   # Mississippi
        1626,  # Missouri
        512,   # Montana
        3837,  # Nebraska
        1634,  # Nevada
        1028,  # New Hampshire
        962,   # New Jersey
        1985,  # New Mexico
        654,   # New York
        1801,  # North Carolina
        1689,  # North Dakota
        1944,  # Ohio
        2027,  # Oklahoma
        2030,  # Oregon
        1116,  # Pennsylvania
        557,   # Rhode Island
        807,   # South Carolina
        1037,  # South Dakota
        289,   # Tennessee
        1316,  # Texas
        288,   # Utah
        1590,  # Vermont
        1615,  # Virginia
        1275,  # Washington
        1650,  # West Virginia
        1601,  # Wisconsin
        9510,  # Wyoming
    ]

# Option 1: Fetch data and save as CSV files (currently commented out)
# Uncomment if you want CSV backups
# fetch_and_save_sensor_data(client,sensor_ids=sensor_ids, datetime_from='1/1/2023', datetime_to='11/13/2025')

# Option 2: Fetch data and save directly to the database (recommended)
utc_today = get_utc_date_today()  # Get today's date in UTC

# Initialize the database (create required tables)
initialize_db()

# Test the database connection
test_db_connection()

# Fetch data and save to database
# Using the location-based method (same as in fetch_data_to_csv.py):
# 1. For each location, get all sensors
# 2. Filter for PM2.5 sensors
# 3. Fetch data for those PM2.5 sensors
# 4. Save to the database (handles duplicate data automatically)
print("\n" + "=" * 70)
print("Starting data fetching (Location-Based Method)")
print("=" * 70)
print(f"Date range: 2024/01/01 to {utc_today.strftime('%Y/%m/%d')}")
print(f"Number of Locations: {len(location_ids)}")
print(f"Parameter: Only fetch PM2.5")
print("=" * 70)

fetch_and_save_to_db(
    client=client,
    location_ids=location_ids,                     # Location IDs (not sensor IDs)
    datetime_from='1/1/2024',                      # Start date (from Jan 1, 2024)
    datetime_to=utc_today.strftime('%m/%d/%Y'),    # End date (today)
    table_name='sensor_data'                       # Target table
)

