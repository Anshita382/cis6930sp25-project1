import requests
import pandas as pd
import duckdb
from datetime import datetime, timezone

# API endpoints
ARRESTS_URL = "https://data.cityofgainesville.org/resource/aum6-79zv.json"
TRAFFIC_CRASHES_URL = "https://data.cityofgainesville.org/resource/iecn-3sxx.json"
CRIME_RESPONSES_URL = "https://data.cityofgainesville.org/resource/gvua-xt9q.json"

def is_future_date(year, month, day):
    requested_date = datetime(year, month, day, tzinfo=timezone.utc)
    current_date = datetime.now(timezone.utc)
    return requested_date > current_date

def fetch_data(url: str, date_field: str, year: int, month: int, day: int):
    """
    Fetch data for the entire day from T00:00:00 to T23:59:59
    """
    date_str = f"{year}-{month:02d}-{day:02d}"
    # e.g. 2025-01-05T00:00:00 to 2025-01-05T23:59:59
    where_clause = f"{date_field} between '{date_str}T00:00:00' and '{date_str}T23:59:59'"
    params = {"$where": where_clause}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return []

def load_and_clean_data(con, data, table_name):
    df = pd.DataFrame(data)
    
    # Define minimal expected schemas for each table if no data is returned
    expected_schemas = {
        'crimes': ["id", "latitude", "longitude", "report_date", "offense_date"],
        'traffic_crashes': ["case_number", "totalpeopleinvolved", "latitude", "longitude", "accident_date"],
        'arrests': ["case_number", "arrest_date"]
    }
    
    if df.empty or len(df.columns) == 0:
        df = pd.DataFrame(columns=expected_schemas.get(table_name, []))
    else:
        if table_name == 'crimes':
            df = df.drop([
                'report_hour_of_day', 'report_day_of_week', 'offense_hour_of_day', 'offense_day_of_week',
                'city', 'state', ':@computed_region_ndi2_bfht', ':@computed_region_ecgy_hwrz',
                ':@computed_region_e6r8_dw75', ':@computed_region_u9vc_vmbc',
                ':@computed_region_4rat_gsiv', ':@computed_region_43jd_v64e',
                ':@computed_region_axii_i744', ':@computed_region_9cfm_spy5'
            ], axis=1, errors='ignore')
            if 'report_date' in df.columns:
                df['report_date'] = pd.to_datetime(df['report_date']).dt.date
            if 'offense_date' in df.columns:
                df['offense_date'] = pd.to_datetime(df['offense_date']).dt.date

        elif table_name == 'traffic_crashes':
            df = df.drop([
                'numberofbicyclesinvolved', 'numberofpedestriansinvolved', 'totalvehiclesinvolved',
                'numberofmopedsinvolved', 'numberofmotorcylesinvolved', 'numberofbusesinvolved',
                'totalfatalities', 'geox', 'geoy', 'location', 'city', 'state',
                ':@computed_region_ecgy_hwrz', ':@computed_region_e6r8_dw75', ':@computed_region_u9vc_vmbc',
                ':@computed_region_4rat_gsiv', ':@computed_region_axii_i744', 'at_street_address',
                'at', 'direction', 'accident_hour_of_day', 'crash_minutes', 
                'accident_day_of_week', 'occurred_on', 'intersecttype', 'at_from_intersection'
            ], axis=1, errors='ignore')
            if 'accident_date' in df.columns:
                df['accident_date'] = pd.to_datetime(df['accident_date']).dt.date
            else:
                print("Warning: 'accident_date' column not found in traffic_crashes data")

            # Convert totalpeopleinvolved to integer (or 0 if conversion fails)
            if 'totalpeopleinvolved' in df.columns:
                df['totalpeopleinvolved'] = pd.to_numeric(df['totalpeopleinvolved'], errors='coerce').fillna(0).astype(int)

        elif table_name == 'arrests':
            df = df.drop(['arrest_day_of_week', 'arr_chrg', 'race', 'sex', 'age'], axis=1, errors='ignore')
            if 'arrest_date' in df.columns:
                df['arrest_date'] = pd.to_datetime(df['arrest_date']).dt.date
            else:
                print("Warning: 'arrest_date' column not found in arrests data")
    
    # Register dataframe in DuckDB
    con.register(f'df_{table_name}', df)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_{table_name}")

def fetch_all_data(year: int, month: int, day: int):
    con = duckdb.connect(database=':memory:')
    
    # Arrests
    arrests = fetch_data(ARRESTS_URL, "arrest_date", year, month, day)
    load_and_clean_data(con, arrests, 'arrests')
    
    # Traffic crashes
    traffic_crashes = fetch_data(TRAFFIC_CRASHES_URL, "accident_date", year, month, day)
    load_and_clean_data(con, traffic_crashes, 'traffic_crashes')
    
    # Crime responses
    crime_responses = fetch_data(CRIME_RESPONSES_URL, "report_date", year, month, day)
    load_and_clean_data(con, crime_responses, 'crimes')
    
    return con
