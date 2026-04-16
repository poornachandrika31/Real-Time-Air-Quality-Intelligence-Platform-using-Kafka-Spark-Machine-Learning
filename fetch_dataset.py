import requests
import pandas as pd
from datetime import datetime, timedelta
import time

API_KEY = "1a165c0bbb133a9e08ec0a178cb9a3c3"
# -----------------------------
# YOUR LOCATIONS
# -----------------------------
locations = [
    "Ghansawangi", "Soraon", "Gangadhara mandal", "Bollapalle",
    "Chengalpattu", "Kako", "Srikakulam", "Hyderabad",
    "Vijayawada", "Etmadpur", "Bagalkote", "Khammam",
    "Jamnagar", "Thanjavur", "Pune", "Coimbatore",
    "Nashik", "Tirupati", "Vellore", "Visakhapatnam",
    "New Delhi", "Bengaluru", "Bhopal"
]

# -----------------------------
# GET LAT/LON FROM CITY
# -----------------------------
def get_lat_lon(city):
    url = "http://api.openweathermap.org/geo/1.0/direct"

    res = requests.get(url, params={
        "q": city,
        "limit": 1,
        "appid": API_KEY
    }).json()

    if res:
        return res[0]["lat"], res[0]["lon"]
    return None, None

# -----------------------------
# FETCH AQI DATA
# -----------------------------
def fetch_aqi(lat, lon):
    end = int(datetime.now().timestamp())
    start = int((datetime.now() - timedelta(days=30)).timestamp())  # ⚠️ API limit

    url = "http://api.openweathermap.org/data/2.5/air_pollution/history"

    res = requests.get(url, params={
        "lat": lat,
        "lon": lon,
        "start": start,
        "end": end,
        "appid": API_KEY
    }).json()

    if "list" not in res:
        return []

    records = []

    for entry in res["list"]:
        records.append({
            "timestamp": datetime.fromtimestamp(entry["dt"]),
            "lat": lat,
            "lon": lon,
            "pm25": entry["components"]["pm2_5"],
            "pm10": entry["components"]["pm10"],
            "aqi": entry["main"]["aqi"]
        })

    return records

# -----------------------------
# MAIN LOOP
# -----------------------------
all_data = []

for city in locations:
    print(f"\n📍 Processing: {city}")

    lat, lon = get_lat_lon(city)

    if lat is None:
        print("❌ Failed to get location")
        continue

    print(f"✔ Coordinates: {lat}, {lon}")

    data = fetch_aqi(lat, lon)

    for row in data:
        row["city"] = city
        all_data.append(row)

    time.sleep(1)  # avoid API limit

# -----------------------------
# SAVE CSV
# -----------------------------
df = pd.DataFrame(all_data)

if not df.empty:
    df.to_csv("aqi_multi_city_dataset.csv", index=False)
    print("\n✅ Dataset saved: aqi_multi_city_dataset.csv")
else:
    print("❌ No data collected")