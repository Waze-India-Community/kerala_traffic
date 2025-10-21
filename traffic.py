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
df['update_ts']=pd.to_datetime(df['updateMillis'],unit='ms',origin='unix', utc=True).dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
df['published_ts']=pd.to_datetime(df['pubMillis'],unit='ms',origin='unix', utc=True).dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)

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
        "traffic_delay_calculated" : row['traffic_delay'],
        "delay" : row['delay'],
        "updated" : row['update_ts'].strftime('%Y%m%d%H%M%S'),
        "published" : row['published_ts'].strftime('%Y%m%d%H%M%S')
    }
    input_list.append(iterlist)

# for location info table 
df_exploded=df2.explode('line')[['uuid','line']]
df_exploded['line'] = df_exploded['line'].apply(lambda d : f"{d['y']},{d['x']}")
loc_list =[]
for idx, row in df_exploded.iterrows():
    cordlist={
        'uuid' : row['uuid'],
        'location' : row['line']
    }
    loc_list.append(cordlist)

# Supabase credentials from environment
url = os.environ["SUPABASE_URL"]
key = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

# Upsert to Supabase
if input_list:
    response = supabase.table("kerala_traffic").upsert(input_list).execute()

if loc_list:
    response = supabase.table("kl_traffic_loc").upsert(loc_list).execute()

