# 🌍 Real-Time Air Quality Intelligence Platform

## 📌 Overview

This project is an end-to-end **real-time data engineering and machine learning pipeline** that collects live air quality data, processes it using distributed systems, and provides interactive visualizations along with predictive insights.

The system integrates **Kafka, Apache Spark, MongoDB, and Streamlit**, along with machine learning models for forecasting and analysis.

---

## 🚀 Key Features

* 🌐 Live AQI data streaming using OpenWeather API
* ⚡ Real-time data ingestion with Kafka
* 🔥 Distributed processing using Apache Spark Streaming
* 🗄 Storage in MongoDB (NoSQL database)
* 🖥 Interactive dashboard using Streamlit
* 📊 KPI metrics and AQI visualization
* 🤖 Machine Learning:

  * Gradient Boosted Trees (Spark ML)
  * Prophet (Time-series forecasting)
* 📈 Prediction metrics (RMSE, MAE, MAPE)
* 🗺 Heatmap and location-based AQI tracking
* 🚨 Peak detection and trend analysis

---

## 🏗 Architecture

```
OpenWeather API → Kafka → Spark Streaming → MongoDB → Streamlit Dashboard
```

---

## ⚙️ Tech Stack

| Component   | Technology                          |
| ----------- | ----------------------------------- |
| Data Source | OpenWeather API                     |
| Streaming   | Apache Kafka                        |
| Processing  | Apache Spark (Structured Streaming) |
| Storage     | MongoDB                             |
| Frontend    | Streamlit                           |
| ML Models   | Spark ML (GBT), Prophet             |
| Language    | Python                              |

---

## 🔄 Pipeline Workflow

1. **Data Collection**

   * Fetch AQI data (PM2.5, PM10) from OpenWeather API every few seconds.

2. **Kafka Producer**

   * Streams real-time data into Kafka topic (`aqi_topic`).

3. **Spark Streaming**

   * Reads data from Kafka
   * Parses and processes JSON
   * Enriches data
   * Stores into MongoDB

4. **MongoDB**

   * Stores processed AQI data with timestamps

5. **Streamlit Dashboard**

   * Displays:

     * AQI Map
     * Heatmap
     * KPIs
     * Leaderboard
     * Forecast charts

---

## 🤖 Machine Learning

### 1. Gradient Boosted Trees (Spark ML)

* Trained on historical AQI dataset
* Learns patterns across cities and time features

### 2. Prophet Model

* Time-series forecasting
* Predicts AQI for next 5 days
* Captures trends and seasonality

---

## 📊 Model Evaluation Metrics

* **RMSE (Root Mean Squared Error)** – measures large errors
* **MAE (Mean Absolute Error)** – average error
* **MAPE (Mean Absolute Percentage Error)** – percentage accuracy

---

## 📈 Dashboard Features

* 📊 Prediction vs Actual comparison
* 📉 Error trend visualization
* 🌍 City-wise performance analysis
* ⚙ System metrics:

  * Latency
  * Throughput
  * Data freshness
* 🚨 Peak detection system

---

## 🖧 Cluster Setup

* Spark Master + Worker architecture
* Supports multi-node cluster (can scale to multiple machines)
* Demonstrates distributed processing capability

---

## ▶️ How to Run

### 1. Start MongoDB

```bash
net start MongoDB
```

### 2. Start Kafka

```bash
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties
```

### 3. Create Kafka Topic

```bash
kafka-topics.sh --create --topic aqi_topic --bootstrap-server <IP>:9092
```

### 4. Start Spark Cluster

```bash
start-master.sh
start-worker.sh spark://<MASTER_IP>:7077
```

### 5. Run Spark Streaming

```bash
spark-submit --master spark://<MASTER_IP>:7077 spark_streaming.py
```

### 6. Run Producer

```bash
python3 kafka_producer.py <lat> <lon>
```

### 7. Run Dashboard

```bash
streamlit run app.py
```

---

## 📌 Future Improvements

* Deploy on cloud (AWS/GCP)
* Add real-time alerts/notifications
* Integrate more environmental data sources
* Improve model accuracy with deep learning

---

## 🏆 Conclusion

This project demonstrates how modern big data tools can be integrated to build a **scalable, real-time analytics system** with machine learning capabilities.

---

