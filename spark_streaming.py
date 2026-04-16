from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, DoubleType, StringType
from pymongo import MongoClient
import requests
from datetime import datetime

# ----------------------------
# CONFIG
# ----------------------------
API_KEY = "1a165c0bbb133a9e08ec0a178cb9a3c3"

# ----------------------------
# MongoDB
# ----------------------------
client = MongoClient("mongodb://11.12.2.119:27017/")
db = client["air_quality"]
collection = db["map_history"]

# ----------------------------
# Spark Session
# ----------------------------
spark = SparkSession.builder \
    .appName("AQI Streaming") \
    .master("spark://11.12.2.119:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Schema (Kafka input)
# ----------------------------
schema = StructType() \
    .add("lat", DoubleType()) \
    .add("lon", DoubleType()) \
    .add("timestamp", StringType()) \
    .add("pm25", DoubleType()) \
    .add("pm10", DoubleType())

# ----------------------------
# Read Kafka
# ----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "11.12.2.119:9092") \
    .option("subscribe", "aqi_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# ----------------------------
# Parse JSON
# ----------------------------
json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# ----------------------------
# PROCESS + STORE (CORE LOGIC)
# ----------------------------
def process_batch(batch_df, batch_id):

    rows = batch_df.collect()

    print(f"\n🔥 Batch received with {len(rows)} rows")

    for row in rows:
        lat = row["lat"]
        lon = row["lon"]

        res = requests.get(
            "http://api.openweathermap.org/data/2.5/air_pollution",
            params={"lat": lat, "lon": lon, "appid": API_KEY}
        ).json()

        if "list" not in res:
            continue

        pm25 = res["list"][0]["components"]["pm2_5"]

        print(f"\n📍 {lat}, {lon}")
        print(f"🌫 PM2.5: {pm25}")

        record = {
            "lat": lat,
            "lon": lon,
            "pm25": pm25,
            "timestamp": datetime.now()
        }

        collection.insert_one(record)

# ----------------------------
# STREAM → MONGO
# ----------------------------
mongo_query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()
# ----------------------------
# RUN STREAM
# ----------------------------
spark.streams.awaitAnyTermination()