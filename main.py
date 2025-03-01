import argparse
import requests
import duckdb
from datetime import datetime
from geopy.distance import geodesic

# ✅ Corrected API URLs
ARRESTS_URL = "https://data.cityofgainesville.org/resource/ktq3-kscm.json"   # Correct URL
TRAFFIC_CRASHES_URL = "https://data.cityofgainesville.org/resource/d6wv-s8u2.json"
CRIME_RESPONSES_URL = "https://data.cityofgainesville.org/resource/cdd4-6ifk.json"

DB_FILE = "mib_data.duckdb"

def fetch_data(url, date_str):
    """Fetch data from Gainesville Open Data API for the given date."""
    params = {
        "$where": f"date_trunc('day', datetime) = '{date_str}'"
    }
    response = requests.get(url, params=params)

    if not response.ok:
        raise Exception(f"Error fetching data from {url} - {response.status_code}")
    
    return response.json()

def store_data_in_duckdb(arrests, crashes, crimes):
    """Store data into DuckDB."""
    con = duckdb.connect(DB_FILE)
    con.execute("CREATE OR REPLACE TABLE arrests AS SELECT * FROM json(?);", (arrests,))
    con.execute("CREATE OR REPLACE TABLE crashes AS SELECT * FROM json(?);", (crashes,))
    con.execute("CREATE OR REPLACE TABLE crimes AS SELECT * FROM json(?);", (crimes,))
    con.close()

def get_all_incidents():
    """Retrieve all incidents with case_number, coordinates, and people count."""
    con = duckdb.connect(DB_FILE)
    query = """
    SELECT case_number, latitude, longitude, COALESCE(total_involved, 0) AS people
    FROM (
        SELECT case_number, latitude, longitude, total_involved FROM arrests
        UNION ALL
        SELECT case_number, latitude, longitude, total_involved FROM crashes
        UNION ALL
        SELECT case_number, latitude, longitude, total_involved FROM crimes
    )
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    incidents = con.execute(query).fetchall()
    con.close()
    return incidents

def calculate_distance(coord1, coord2):
    """Calculate geodesic distance in kilometers."""
    return geodesic(coord1, coord2).kilometers

def find_incidents_within_radius(incidents, center, radius_km=1.0):
    """✅ This function was missing — it's now added correctly."""
    nearby = []
    for case_number, lat, lon, people in incidents:
        if lat is None or lon is None:
            continue
        distance = calculate_distance(center, (float(lat), float(lon)))
        if distance <= radius_km:
            nearby.append((people, case_number))
    return nearby

def process_data(year, month, day):
    date_str = f"{year}-{month:02d}-{day:02d}"

    # ✅ Fetch data from APIs for the given date
    arrests = fetch_data(ARRESTS_URL, date_str)
    crashes = fetch_data(TRAFFIC_CRASHES_URL, date_str)
    crimes = fetch_data(CRIME_RESPONSES_URL, date_str)

    if not (arrests or crashes or crimes):
        # ✅ No data found for this date — print nothing and return
        return

    # ✅ Store everything into DuckDB
    store_data_in_duckdb(arrests, crashes, crimes)

    # ✅ Fetch combined incident data
    incidents = get_all_incidents()

    if not incidents:
        return  # No valid incidents with coordinates found

    # ✅ Find incident with the **most people involved**
    most_people_incident = max(incidents, key=lambda x: x[3])
    center_location = (float(most_people_incident[1]), float(most_people_incident[2]))

    # ✅ Find all incidents within 1km of this incident
    nearby_incidents = find_incidents_within_radius(incidents, center_location)

    # ✅ Sort by people descending, then case_number ascending
    nearby_incidents.sort(key=lambda x: (-x[0], x[1]))

    # ✅ Output result as required
    for people, case_number in nearby_incidents:
        print(f"{people}\t{case_number}")

def main():
    parser = argparse.ArgumentParser(description="MIB Incident Detector - Canvassing the Scene")
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)
    parser.add_argument("--day", required=True, type=int)
    args = parser.parse_args()

    process_data(args.year, args.month, args.day)

if __name__ == "__main__":
    main()
