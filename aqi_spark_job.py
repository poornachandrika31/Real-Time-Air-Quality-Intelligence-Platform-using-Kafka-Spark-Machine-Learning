from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import json
import requests
import time
from kafka import KafkaProducer
from pymongo import MongoClient

# -----------------------------
# CONNECT TO SPARK CLUSTER
# -----------------------------
spark = SparkSession.builder \
    .appName("AQI Kafka + Spark Integration") \
    .master("spark://0.0.0.0:7077") \
    .getOrCreate()

print("🔥 SPARK JOB STARTED")

# -----------------------------
# WAIT FOR KAFKA MESSAGE
# -----------------------------
time.sleep(5)

# -----------------------------
# READ FROM KAFKA
# -----------------------------
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "aqi-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# -----------------------------
# PARSE KAFKA DATA
# -----------------------------
rows = kafka_df.selectExpr("CAST(value AS STRING) as json").collect()

records = []

for row in rows:
    if row["json"]:
        msg = json.loads(row["json"])

        lat = msg["lat"]
        lon = msg["lon"]

        print(f"📍 Processing location: {lat}, {lon}")

        # -----------------------------
        # FETCH API DATA
        # -----------------------------
        API_KEY = "1a165c0bbb133a9e08ec0a178cb9a3c3"
        url = "http://api.openweathermap.org/data/2.5/air_pollution"

        params = {
            "lat": lat,
            "lon": lon,
            "appid": API_KEY
        }

        response = requests.get(url, params=params)
        data = response.json()

        if "list" in data:
            entry = data["list"][0]

            records.append({
                "timestamp": str(datetime.now()),
                "lat": lat,
                "lon": lon,
                "aqi": entry["main"]["aqi"],
                "pm25": entry["components"]["pm2_5"],
                "pm10": entry["components"]["pm10"]
            })

# -----------------------------
# CHECK DATA
# -----------------------------
print("🔥 RECORDS:", records)

if not records:
    print("❌ No data from Kafka")
    exit()

# -----------------------------
# PROCESS USING SPARK
# -----------------------------
df = spark.createDataFrame(records)

df = df.withColumn("pm25_scaled", col("pm25") * 1.1)

print("📊 Data processed on cluster:")
df.show(truncate=False)

# -----------------------------
# SEND TO MULTIPLE KAFKA TOPICS
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for row in df.collect():
    data = row.asDict()

    print("👉 Sending:", data)

    producer.send("aqi-analytics", data)

    if data.get("aqi", 0) > 150:
        producer.send("aqi-alerts", data)

    producer.send("system-logs", {
        "status": "processed",
        "lat": data.get("lat"),
        "lon": data.get("lon")
    })

producer.flush()
print("✅ Sent to all Kafka topics")

# -----------------------------
# STORE IN MONGODB
# -----------------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["air_quality"]
collection = db["aqi_data"]

for row in df.collect():
    collection.insert_one(row.asDict())

print("💾 Stored in MongoDB")

# -----------------------------
# STOP SPARK
# -----------------------------
spark.stop()
