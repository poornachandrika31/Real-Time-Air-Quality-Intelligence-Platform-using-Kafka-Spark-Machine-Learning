from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import PipelineModel
from datetime import datetime, timedelta
from pymongo import MongoClient
import sys

# -----------------------------
# START SPARK
# -----------------------------
spark = SparkSession.builder \
    .appName("AQI Prediction") \
    .getOrCreate()

# -----------------------------
# LOAD MODEL
# -----------------------------
model = PipelineModel.load("spark_aqi_model")

# -----------------------------
# LOAD DATASET (for model context)
# -----------------------------
df = spark.read.csv(
    "aqi_multi_city_dataset.csv",
    header=True,
    inferSchema=True
)

# -----------------------------
# INPUT FROM DASHBOARD
# -----------------------------
lat_input = float(sys.argv[1])
lon_input = float(sys.argv[2])

print(f"\n📍 User clicked location: {lat_input}, {lon_input}")

# -----------------------------
# FIND NEAREST CITY (MODEL PURPOSE ONLY)
# -----------------------------
df = df.withColumn(
    "distance",
    (col("lat") - lat_input)**2 + (col("lon") - lon_input)**2
)

nearest = df.orderBy("distance").first()

model_city = nearest["city"]

print(f"📍 Model uses city: {model_city}")

# -----------------------------
# GET LAST TREND INDEX (IMPORTANT FIX)
# -----------------------------
city_df = df.filter(col("city") == model_city)
last_t = city_df.count()

# -----------------------------
# CREATE FUTURE DATA (CORRECT 5 DAYS)
# -----------------------------
base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

future_rows = []

for i in range(1, 6):  # next 5 days
    dt = base_date + timedelta(days=i)

    future_rows.append((
        model_city,
        dt,
        last_t + i,   # ✅ correct trend continuation
        12,           # fixed hour (stable prediction)
        dt.day,
        dt.month
    ))

future_df = spark.createDataFrame(
    future_rows,
    ["city", "timestamp", "t", "hour", "day", "month"]
)

print("\n📊 Future input:")
future_df.show()

# -----------------------------
# PREDICT
# -----------------------------
predictions = model.transform(future_df)

print("\n📊 Predictions:")
predictions.select("timestamp", "prediction").show()

# -----------------------------
# CONVERT TO PANDAS
# -----------------------------
pred_pd = predictions.select("timestamp", "prediction").toPandas()

if pred_pd.empty:
    print("❌ ERROR: No predictions generated!")
    exit()

# -----------------------------
# STORE IN MONGODB
# -----------------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["air_quality"]
collection = db["predictions"]

results = []

for _, row in pred_pd.iterrows():
    record = {
        "model_city": model_city,        # for model reference
        "lat": float(lat_input),         # actual clicked location
        "lon": float(lon_input),
        "timestamp": row["timestamp"],
        "predicted_aqi": float(row["prediction"]),
        "created_at": datetime.now()
    }

    collection.insert_one(record)
    results.append(record)

# -----------------------------
# OUTPUT
# -----------------------------
print("\n📈 5-Day AQI Forecast:")
for r in results:
    print(r)

print("\n✅ Stored in MongoDB successfully!")