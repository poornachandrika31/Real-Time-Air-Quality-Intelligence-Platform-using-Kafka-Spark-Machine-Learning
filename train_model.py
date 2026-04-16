from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofmonth, month, row_number
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline

# -----------------------------
# START SPARK
# -----------------------------
spark = SparkSession.builder \
    .appName("AQI Spark ML Training") \
    .getOrCreate()

# -----------------------------
# LOAD DATA
# -----------------------------
df = spark.read.csv("aqi_multi_city_dataset.csv", header=True, inferSchema=True)

# -----------------------------
# PREPROCESS
# -----------------------------
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Time features
df = df.withColumn("hour", hour("timestamp"))
df = df.withColumn("day", dayofmonth("timestamp"))
df = df.withColumn("month", month("timestamp"))

# Time index per city
window = Window.partitionBy("city").orderBy("timestamp")
df = df.withColumn("t", row_number().over(window))

# -----------------------------
# ENCODE CITY
# -----------------------------
indexer = StringIndexer(inputCol="city", outputCol="city_index")

# -----------------------------
# FEATURE VECTOR
# -----------------------------
assembler = VectorAssembler(
    inputCols=["t", "hour", "day", "month", "city_index"],
    outputCol="features"
)

# -----------------------------
# MODEL (Spark ML)
# -----------------------------
model = GBTRegressor(
    featuresCol="features",
    labelCol="pm25",
    maxIter=50
)

# -----------------------------
# PIPELINE
# -----------------------------
pipeline = Pipeline(stages=[indexer, assembler, model])

# -----------------------------
# TRAIN
# -----------------------------
model_pipeline = pipeline.fit(df)

# -----------------------------
# SAVE MODEL
# -----------------------------
model_pipeline.write().overwrite().save("spark_aqi_model")

print("✅ Spark ML model trained and saved!")