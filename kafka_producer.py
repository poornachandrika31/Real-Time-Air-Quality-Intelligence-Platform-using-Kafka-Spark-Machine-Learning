from kafka import KafkaProducer
import json
import time
import requests
from datetime import datetime
import sys

# -----------------------------
# CONFIG
# -----------------------------
API_KEY = "1a165c0bbb133a9e08ec0a178cb9a3c3"

# -----------------------------
# KAFKA PRODUCER
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers="11.12.2.119:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -----------------------------
# STREAM FUNCTION
# -----------------------------
def start_stream(lat, lon):
    print(f"🚀 Starting Kafka stream for {lat}, {lon}\n")

    while True:
        try:
            res = requests.get(
                "http://api.openweathermap.org/data/2.5/air_pollution",
                params={
                    "lat": lat,
                    "lon": lon,
                    "appid": API_KEY
                }
            ).json()

            if "list" in res:
                data = res["list"][0]

                message = {
                    "lat": float(lat),
                    "lon": float(lon),
                    "timestamp": str(datetime.now()),
                    "pm25": float(data["components"]["pm2_5"]),
                    "pm10": float(data["components"]["pm10"])
                }

                # ✅ send to Kafka topic
                producer.send("aqi_stream", value=message)

                print("📤 Sent:", message)

            else:
                print("⚠️ No data received from API")

        except Exception as e:
            print("❌ Error:", e)

        time.sleep(5)  # every 5 seconds


# -----------------------------
# ENTRY POINT (IMPORTANT 🔥)
# -----------------------------
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("❌ Usage: python kafka_producer.py <lat> <lon>")
        sys.exit(1)

    lat = float(sys.argv[1])
    lon = float(sys.argv[2])

    start_stream(lat, lon)