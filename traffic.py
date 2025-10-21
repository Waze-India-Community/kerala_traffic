import os
import json
import requests
import pandas as pd
from supabase import create_client, Client
from shapely.geometry import LineString

# Bounding box for Kerala
top = 12.22455530
bottom = 8.31839747
left = 75.23357871
right = 77.18940846

# Fetch Waze traffic data
url = f"https://www.waze.com/live-map/api/georss?top={top}&bottom={bottom}&left={left}&right={right}&env=row&types=traffic"
r = requests.get(url)
data = r.json()
jams = data.get('jams', [])

# Filter out road closures
filtered = [item for item in jams if item.get('causeAlert', {}).get('type') != 'ROAD_CLOSED']
df = pd.DataFrame(filtered)

# Compute traffic severity and delay
df['traffic_delay'] = (df['length'] / 1000) / df['speedKMH'] * 60
df['update_ts']=pd.to_datetime(df['updateMillis'],unit='ms',origin='unix', utc=True).dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
df['published_ts']=pd.to_datetime(df['pubMillis'],unit='ms',origin='unix', utc=True).dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)

# Filter for significant jams
df2 = df[(df['level'] >= 1) & (df['length'] > 50)]

#Linestring creation
def list_to_wkt_linestring(coords_list):
    """Converts a Python list of coordinate dictionaries into a WKT LINESTRING string."""
    
    if not isinstance(coords_list, list) or not coords_list:
        return None
    
    try:
        # 2. Extract coordinates as a list of (lon, lat) tuples
        #    This step remains the same as it correctly handles the list structure:
        coordinates = [(d['x'], d['y']) for d in coords_list]
        
        # 3. Create a shapely LineString object
        line_obj = LineString(coordinates)
        
        # 4. Convert the object to WKT format
        return line_obj.wkt
    
    except Exception as e:
        # Handle cases where keys 'x' or 'y' are missing, etc.
        print(f"Error processing coordinates: {e}")
        return None
df2['wkt_path'] = df2['line'].apply(list_to_wkt_linestring)

# Format for Supabase
input_list = []
for idx, row in df2.iterrows():
    iterlist={
        "uuid" : row['uuid'],
        "city" : row['city'],
        "street" : str(row['street']).replace("Rd","Road"),
        "length" : row['length'],
        "speed" : row['speedKMH'],
        "road_type" : row['roadType'],
        "start_jn" : str(row['startNode']).replace("nan","").replace("Rd", "Road"),
        "end_jn" : str(row['endNode']).replace("nan","").replace("Rd", "Road"),
        "severity" : row['level'],
        "line" : row['wkt_path'],
        "traffic_delay_calculated" : row['traffic_delay'],
        "delay" : row['delay'],
        "updated" : row['update_ts'].strftime('%Y%m%d%H%M%S'),
        "published" : row['published_ts'].strftime('%Y%m%d%H%M%S')
    }
    input_list.append(iterlist)

# Supabase credentials from environment
url = os.environ["SUPABASE_URL"]
key = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

# Upsert to Supabase
if input_list:
    response = supabase.table("kerala_traffic").upsert(input_list).execute()
    print(f"Upserted {len(input_list)} records.")
else:
    print("Nothing to upsert.")
