import pandas as pd

def get_sensor_data(client, sensor_id, datetime_from="2020-01-01T00:00:00Z", datetime_to="2025-01-01T00:00:00Z"):
    print("get_sensor_data() called")
    print(f"client={client}, sensor_id={sensor_id}")
    print(f"datetime_from={datetime_from}, datetime_to={datetime_to}")

    # create fake data
    fake_data = [
        {
            "sensor_id": sensor_id,
            "parameter": "pm25",
            "datetime_utc": "2024-01-01T00:00:00Z",
            "datetime_local": "2024-01-01T08:00:00Z",
            "value": 10.5,
            "units": "µg/m³",
            "coverage_percent": 100.0,
            "min": 8.0,
            "max": 12.0,
            "median": 10.0,
        },
        {
            "sensor_id": sensor_id,
            "parameter": "pm25",
            "datetime_utc": "2024-01-02T00:00:00Z",
            "datetime_local": "2024-01-02T08:00:00Z",
            "value": 11.0,
            "units": "µg/m³",
            "coverage_percent": 100.0,
            "min": 9.0,
            "max": 14.0,
            "median": 11.0,
        },
    ]

    # 返回 DataFrame
    df = pd.DataFrame(fake_data)
    return df