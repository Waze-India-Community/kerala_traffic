import os
import json
import requests
import pandas as pd
from supabase import create_client, Client

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

# Filter for significant jams
df2 = df[(df['level'] >= 1) & (df['length'] > 50)]

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
        "line" : row['line'],
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
