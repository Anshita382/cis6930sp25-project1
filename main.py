import argparse
import math
import warnings
# from data_extractor import fetch_all_data, is_future_date
from data_extractor import fetch_all_data, is_future_date

# Suppress deprecation warnings related to NumPy internals
warnings.filterwarnings("ignore", category=DeprecationWarning)

def get_incident_location(incident):
    try:
        lat = float(incident.get("latitude", ""))
        lon = float(incident.get("longitude", ""))
        return (lat, lon)
    except (ValueError, TypeError):
        return None

def get_total_people(incident):
    if "totalpeopleinvolved" in incident:
        try:
            return int(incident["totalpeopleinvolved"])
        except ValueError:
            return 0
    if "arrest_date" in incident or "offense_date" in incident:
        return 1
    return 0

def get_case_number(incident):
    if "case_number" in incident:
        return incident["case_number"]
    elif "dhsmv_number" in incident:
        return incident["dhsmv_number"]
    elif "id" in incident:
        return incident["id"]
    return ""

def parse_arguments():
    parser = argparse.ArgumentParser(description="CIS 6930 Project 1: Canvassing the Scene")
    parser.add_argument("--year", type=int, required=True, help="Year of incident")
    parser.add_argument("--month", type=int, required=True, help="Month of incident")
    parser.add_argument("--day", type=int, required=True, help="Day of incident")
    return parser.parse_args()

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth's radius in kilometers
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def main():
    args = parse_arguments()
    
    if is_future_date(args.year, args.month, args.day):
        print(f"Error: Requested date {args.year}-{args.month:02d}-{args.day:02d} is in the future. No data available.")
        return
    
    con = fetch_all_data(args.year, args.month, args.day)
    
    # Register the haversine function as a UDF in DuckDB
    con.create_function("haversine", haversine, [float, float, float, float], float)
    
    # Check if there are any traffic crashes; if none, output nothing
    traffic_count = con.execute("SELECT count(*) FROM traffic_crashes").fetchone()[0]
    if traffic_count == 0:
        return

    # Identify the reference traffic incident (highest totalpeopleinvolved)
    query_ref = """
    SELECT latitude::float AS ref_lat, longitude::float AS ref_lon 
    FROM traffic_crashes
    ORDER BY totalpeopleinvolved::int DESC
    LIMIT 1;
    """
    ref = con.execute(query_ref).fetchone()
    if ref is None:
        return

    ref_lat, ref_lon = ref

    # Combine incidents from traffic, crimes, and arrests.
    # According to the new requirement, we sort by number of people desc, then case_number desc
    query_combined = f"""
    WITH combined AS (
        SELECT CAST(case_number AS VARCHAR) AS case_number,
               CAST(totalpeopleinvolved AS INT) AS num_people,
               CAST(latitude AS FLOAT) AS latitude,
               CAST(longitude AS FLOAT) AS longitude
        FROM traffic_crashes
        UNION ALL
        SELECT CAST(id AS VARCHAR) AS case_number,
               1 AS num_people,
               CAST(latitude AS FLOAT) AS latitude,
               CAST(longitude AS FLOAT) AS longitude
        FROM crimes
        UNION ALL
        SELECT CAST(a.case_number AS VARCHAR) AS case_number,
               1 AS num_people,
               CAST(t.latitude AS FLOAT) AS latitude,
               CAST(t.longitude AS FLOAT) AS longitude
        FROM arrests a
        JOIN traffic_crashes t 
          ON CAST(a.case_number AS VARCHAR) = CAST(t.case_number AS VARCHAR)
    )
    SELECT num_people, case_number
    FROM combined, (SELECT {ref_lat} AS ref_lat, {ref_lon} AS ref_lon) AS ref
    WHERE haversine(latitude, longitude, ref.ref_lat, ref.ref_lon) <= 1
    ORDER BY num_people DESC, case_number DESC;
    """
    
    results = con.execute(query_combined).fetchall()
    
    for row in results:
        print(f"{row[0]}\t{row[1]}")

if __name__ == "__main__":
    main()
