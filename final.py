import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from prophet import Prophet
from pymongo import MongoClient
import folium
from folium.plugins import HeatMap
from streamlit_folium import st_folium
import subprocess
import time

# -----------------------------
# CONFIG
# -----------------------------
API_KEY = "1a165c0bbb133a9e08ec0a178cb9a3c3"
st.set_page_config(layout="wide")
st.title("🌍 India AQI Intelligence Platform")

# -----------------------------
# MongoDB
# -----------------------------
client = MongoClient("mongodb://11.12.2.119:27017/")
db = client["air_quality"]
collection = db["map_history"]


# -----------------------------
# MAP DATA
# -----------------------------
pipeline = [
    {"$sort": {"timestamp": -1}},
    {
        "$group": {
            "_id": {"lat": "$lat", "lon": "$lon"},
            "pm25": {"$first": "$pm25"},
            "city": {"$first": "$city"}
        }
    }
]

results = list(collection.aggregate(pipeline))

# -----------------------------
# KPI
# -----------------------------
if results:
    pm_vals = [r["pm25"] for r in results if r.get("pm25")]

    if pm_vals:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Avg PM2.5", f"{sum(pm_vals)/len(pm_vals):.1f}")
        c2.metric("Best", f"{min(pm_vals):.1f}")
        c3.metric("Worst", f"{max(pm_vals):.1f}")
        c4.metric("Locations", len(pm_vals))

st.divider()

# -----------------------------
# MAP
# -----------------------------
st.subheader("🗺 Smart AQI Map")

m = folium.Map(location=[22.97, 78.65], zoom_start=5)

heat_data = []

for r in results:
    if r.get("pm25"):
        lat = r["_id"]["lat"]
        lon = r["_id"]["lon"]
        pm = r["pm25"]

        heat_data.append([lat, lon, pm])

        color = "green" if pm <= 35 else "orange" if pm <= 55 else "red"

        folium.CircleMarker(
            location=[lat, lon],
            radius=7,
            color=color,
            fill=True,
            fill_opacity=0.8,
            popup=f"{r['city']} | {pm:.1f}"
        ).add_to(m)

if heat_data:
    HeatMap(heat_data, radius=15).add_to(m)

map_data = st_folium(
    m,
    height=500,
    width=1000,
    returned_objects=["last_clicked"]
)

# -----------------------------
# CLICK
# -----------------------------
if map_data and map_data.get("last_clicked"):
    st.session_state["lat"] = map_data["last_clicked"]["lat"]
    st.session_state["lon"] = map_data["last_clicked"]["lng"]

# -----------------------------
# PIPELINE
# -----------------------------
st.subheader("⚡ Kafka + Spark")

if "lat" in st.session_state:

    lat = st.session_state["lat"]
    lon = st.session_state["lon"]

    st.info(f"📍 {lat:.4f}, {lon:.4f}")

    if st.button("🚀 Run Pipeline"):

        subprocess.Popen([
            "python3",
            "/home/chandrika/aqi_1 - Copy/dashboard/kafka_producer.py",
            str(lat),
            str(lon)
        ],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

        time.sleep(3)

        subprocess.Popen([
            "spark-submit",
            "--master", "spark://11.12.2.119:7077",
            "--packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "/home/chandrika/aqi_1 - Copy/streaming/spark_streaming.py"
        ])

        st.success("✅ Pipeline Completed")

st.divider()

# -----------------------------
# ANALYSIS
# -----------------------------
if "lat" in st.session_state:

    lat = st.session_state["lat"]
    lon = st.session_state["lon"]

    # -----------------------------
    # SAFE GEO
    # -----------------------------
    geo = requests.get(
        "http://api.openweathermap.org/geo/1.0/reverse",
        params={"lat": lat, "lon": lon, "limit": 1, "appid": API_KEY}
    ).json()

    if geo and len(geo) > 0 and "name" in geo[0]:
        city = geo[0]["name"]
    else:
        city = "Unknown Location"

    st.header(f"📌 {city}")

    # -----------------------------
    # CURRENT AQI
    # -----------------------------
    current = requests.get(
        "http://api.openweathermap.org/data/2.5/air_pollution",
        params={"lat": lat, "lon": lon, "appid": API_KEY}
    ).json()

    if "list" in current:
        pm25 = current["list"][0]["components"]["pm2_5"]

        st.metric("PM2.5", f"{pm25:.1f}")

        if pm25 <= 35:
            st.success("🟢 Good")
        elif pm25 <= 55:
            st.warning("🟡 Moderate")
        else:
            st.error("🔴 Unhealthy")

    # -----------------------------
    # LAST 10 DAYS (CORRECT)
    # -----------------------------
    end = int(datetime.now().timestamp())
    start = int((datetime.now() - timedelta(days=10)).timestamp())

    history = requests.get(
        "http://api.openweathermap.org/data/2.5/air_pollution/history",
        params={"lat": lat, "lon": lon, "start": start, "end": end, "appid": API_KEY}
    ).json()

    if "list" in history:

        records = []

        for entry in history["list"]:
            ts = datetime.fromtimestamp(entry["dt"])
            pm = entry["components"]["pm2_5"]

            records.append({"timestamp": ts, "pm25": pm})

        df = pd.DataFrame(records)

        st.subheader("📊 Last 10 Days")

        col1, col2 = st.columns(2)

        with col1:
            st.line_chart(df.set_index("timestamp")["pm25"])

        with col2:
            st.bar_chart(df["pm25"])

        # -----------------------------
        # FORECAST (5 DAYS)
        # -----------------------------
        model_df = df.rename(columns={"timestamp": "ds", "pm25": "y"})

        model = Prophet()
        model.fit(model_df)

        future = model.make_future_dataframe(periods=5)
        forecast = model.predict(future)

        pred = forecast[["ds", "yhat"]].tail(5)

        st.subheader("📈 5-Day Forecast")

        st.line_chart(pred.set_index("ds"))

        st.dataframe(pred)

        # -----------------------------
        # INSIGHTS
        # -----------------------------
        st.subheader("🧠 Insights")

        peak = pred.loc[pred["yhat"].idxmax()]
        st.info(f"Peak on: {peak['ds'].date()}")

        trend = pred["yhat"].iloc[-1] - pred["yhat"].iloc[0]

        if trend > 0:
            st.warning("📈 Pollution increasing")
        else:
            st.success("📉 Improving")

        # -----------------------------
        # ALERTS
        # -----------------------------
        st.subheader("🚨 Alerts")

        avg = pred["yhat"].mean()

        if avg <= 35:
            st.success("🟢 Safe")
        elif avg <= 55:
            st.warning("🟡 Moderate")
        elif avg <= 150:
            st.error("🟠 Unhealthy")
        else:
            st.error("🔴 Hazardous")

st.divider()



