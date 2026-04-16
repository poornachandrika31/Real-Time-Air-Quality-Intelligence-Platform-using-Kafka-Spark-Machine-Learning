import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from pymongo import MongoClient
from sklearn.metrics import mean_squared_error, mean_absolute_error

# -----------------------------
# CONFIG
# -----------------------------
st.set_page_config(layout="wide")
st.title("📊 AQI Model & System Metrics Dashboard")

# -----------------------------
# MongoDB
# -----------------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["air_quality"]
collection = db["map_history"]

# -----------------------------
# FETCH DATA
# -----------------------------
data = list(collection.find().sort("timestamp", 1))

if len(data) < 20:
    st.warning("Not enough data for metrics. Click more cities first.")
    st.stop()

df = pd.DataFrame(data)
df["timestamp"] = pd.to_datetime(df["timestamp"])

# -----------------------------
# SIMULATE PREDICTIONS (for demo)
# -----------------------------
df["predicted"] = df["pm25"].rolling(3).mean().fillna(method="bfill")

actual = df["pm25"]
predicted = df["predicted"]

# -----------------------------
# METRICS CALCULATION
# -----------------------------
rmse = np.sqrt(mean_squared_error(actual, predicted))
mae = mean_absolute_error(actual, predicted)
mape = np.mean(np.abs((actual - predicted) / actual)) * 100

# -----------------------------
# KPI CARDS
# -----------------------------
st.subheader("📊 Prediction Metrics")

c1, c2, c3 = st.columns(3)

c1.metric("RMSE", f"{rmse:.2f}")
c2.metric("MAE", f"{mae:.2f}")
c3.metric("MAPE (%)", f"{mape:.2f}%")

# -----------------------------
# ERROR DISTRIBUTION GRAPH
# -----------------------------
st.subheader("📉 Prediction Error Trend")

df["error"] = actual - predicted
st.line_chart(df.set_index("timestamp")["error"])

# -----------------------------
# ACTUAL vs PREDICTED
# -----------------------------
st.subheader("📈 Actual vs Predicted")

compare_df = df.set_index("timestamp")[["pm25", "predicted"]]
st.line_chart(compare_df)

# -----------------------------
# SYSTEM METRICS (SIMULATED)
# -----------------------------
st.subheader("⚙ System Metrics")

# Simulated values for demo
latency = np.random.uniform(0.5, 2.0)   # seconds
throughput = np.random.randint(50, 150) # records/sec

latest_time = df["timestamp"].max()
freshness = (datetime.now() - latest_time).seconds

c4, c5, c6 = st.columns(3)

c4.metric("Latency (sec)", f"{latency:.2f}")
c5.metric("Throughput (records/sec)", throughput)
c6.metric("Data Freshness (sec)", freshness)

# -----------------------------
# CITY-WISE ERROR
# -----------------------------
st.subheader("🌍 Error per City")

city_error = df.groupby("city").apply(
    lambda x: mean_absolute_error(x["pm25"], x["predicted"])
).reset_index(name="MAE")

st.bar_chart(city_error.set_index("city"))

# -----------------------------
# PEAK DETECTION (SMART)
# -----------------------------
st.subheader("🚨 Peak Detection")

peak_actual = df.loc[df["pm25"].idxmax()]
peak_pred = df.loc[df["predicted"].idxmax()]

st.write(f"Actual Peak: {peak_actual['pm25']:.1f}")
st.write(f"Predicted Peak: {peak_pred['predicted']:.1f}")

if abs(peak_actual["pm25"] - peak_pred["predicted"]) < 10:
    st.success("✅ Model captured peak correctly")
else:
    st.warning("⚠ Model missed peak accuracy")

# -----------------------------
# RAW DATA (OPTIONAL)
# -----------------------------
st.subheader("📄 Raw Data Preview")
st.dataframe(df.tail(20), width="stretch")
