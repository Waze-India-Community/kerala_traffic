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
severity_scale = {1: "Light traffic", 2: "Moderate traffic", 3: "Heavy traffic", 4: "Bumper to bumper traffic", 5: "Standstill traffic"}
df['traffic_delay'] = (df['length'] / 1000) / df['speedKMH'] * 60
df['traffic'] = df['level'].map(severity_scale)
df['ts'] = df['updateMillis']

# Generate human-readable description
def sit_rep(row):
    city = row['city']
    start = row['startNode']
    end = row['endNode']
    street_org = row['street']
    street = street_org.replace("Rd", "Road") if isinstance(street_org, str) and street_org.endswith("Rd") else street_org or "streets"
    traffic = row['traffic']
    speed = int(round(row['speedKMH'], 0))
    delay = int(round(row['traffic_delay'], 0))

    if city and isinstance(city, str) and len(city) > 1:
        if city in [start, end]:
            return f"{traffic} on {street}. Expect a delay of {delay} minutes"
        elif start == end:
            return f"{traffic} on {street} with an expected delay of {delay} minutes"
        elif start and not pd.isna(start):
            return f"{traffic} on {street} from {start} with an expected delay of {delay} minutes"
        elif end and not pd.isna(end):
            return f"{traffic} on {street} towards {end} with an expected delay of {delay} minutes"
    else:
        if start == end:
            return f"{traffic} on {street} with an expected delay of {delay} minutes"
        elif start and end:
            return f"{traffic} on {street} from {start} to {end} with an expected delay of {delay} minutes"
        elif end:
            return f"{traffic} on {street} towards {end} with an expected traffic time of {delay} minutes"
    return f"{traffic} on {street} with an expected delay of {delay} minutes"

df['description'] = df.apply(sit_rep, axis=1)

# Filter for significant jams
df2 = df[(df['level'] >= 3) & (df['city'] != "") & (df['length'] > 50)]

# Format for Supabase
input_list = []
for _, row in df2.iterrows():
    input_list.append({
        "uuid": row['uuid'],
        "city": row['city'],
        "street": str(row['street']).replace("Rd", "Road"),
        "start_jn": str(row['startNode']).replace("nan", "").replace("Rd", "Road"),
        "end_jn": str(row['endNode']).replace("nan", "").replace("Rd", "Road"),
        "traffic": row['level'],
        "description": row['description'],
        "traffic_delay": row['traffic_delay'],
        "event_time": row['ts']
    })

# Supabase credentials from environment
url = os.environ["SUPABASE_URL"]
key = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

# Upsert to Supabase
if input_list:
    response = supabase.table("kl_traffic").upsert(input_list).execute()
    print(f"Upserted {len(input_list)} records.")
else:
    print("Nothing to upsert.")
