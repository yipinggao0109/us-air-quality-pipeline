from openaq import OpenAQ
import pandas as pd
import os
from dotenv import load_dotenv
from dateutil import parser 
from datetime import datetime
from fetch_data_modules import *

# Load API key from .env file in the project root
load_dotenv()
api_key = os.getenv("OPENAQ_API_KEY")
client = OpenAQ(api_key=api_key)

# Ensure output directory exists
os.makedirs('fetched_data/sensor_data', exist_ok=True)

# List of sensor IDs to fetch data for
sensor_ids = [1671,1404,564,8830,2183,2151,1560,1951,2090,1398,8864,314,1619,1155,1754,
            452,7003,1772,326,3837,1634,1028,1985,654,1944,2027,2030,1116,557,807,1037,
            289,1316,288,1615,1275,1650,1601]

# fetch data for each sensor and save to CSV, uncomment to run
# fetch_and_save_sensor_data(client,sensor_ids=sensor_ids, datetime_from='1/1/2023', datetime_to='11/13/2025')

# fetch data for each sensor and save to DB, uncomment to run
utc_today = get_utc_date_today()

initialize_db()
test_db_connection()
fetch_and_save_to_db(
    client=client,
    sensor_ids=sensor_ids,
    datetime_from='1/1/2023',
    datetime_to=utc_today.strftime('%m/%d/%Y'),
    table_name='sensor_data'
)

