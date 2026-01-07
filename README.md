# Spark Data Skew Optimization on Databricks
# 🔍 Overview

This project demonstrates an end-to-end Apache Spark data processing pipeline on Databricks, with a strong focus on data skew detection, shuffle optimization, and performance tuning.

It simulates a real-world big data scenario using:
- Open REST APIs (JSON)
- Text-based dimension data (CSV)
- Automated Spark ETL pipelines

This project is designed for big data beginners while covering production-level optimization techniques frequently discussed in Spark interviews.

# 🏗 Architecture

Open API (User Carts JSON)
          │
          ▼
ODS (Raw Data Layer)
          │
          ▼
DWD (Cleaned & Flattened Data)
          │
          ▼
DWS (Aggregated Metrics)
          │
          ▼
Databricks Jobs (Scheduled)
          │
          ▼
Spark UI & Metrics (Monitoring)

# 📂 Data Sources
# User Behavior Data

- Source: https://dummyjson.com/carts
- Format: Nested JSON
- Challenge: Highly skewed userId distribution

# Product Dimension Data

- Format: CSV
- Role: Small dimension table used for broadcast joins

# ⚠️ Data Skew Detection

- Analyzed key distribution using groupBy

- Verified skew through Spark UI:
  - Long-running tasks
  - Uneven shuffle read/write across partitions

# 🚀 Optimization Techniques
# Data Skew Handling
- Key salting with two-phase aggregation
- Adaptive Query Execution (AQE)

# Shuffle Optimization
- Broadcast join for small dimension tables
- Tuning spark.sql.shuffle.partitions
- Strategic repartitioning and caching

# 🤖 Automation

- Modular ETL functions
- Databricks Jobs for:
  - Scheduling
  - Retry handling
  - Parameterized execution

# 📊 Performance Results
Metric	Before Optimization	After Optimization
Job Duration	~15 min	~4 min
Shuffle Read	~8 GB	~2 GB
Max Task Time	~600s	~120s

# ▶️ How to Run
1. Upload products.csv to Databricks DBFS
2. Create a Databricks cluster (Spark 3.x)
3. Import notebooks from /notebooks
4. Execute notebooks sequentially or via Databricks Jobs

# 🧠 Key Takeaways

- Practical handling of data skew in Spark
- Deep understanding of shuffle mechanics
- Experience with Spark UI performance troubleshooting
- Production-style Databricks pipeline design

# 👤 Author

Taylor Han

Big Data / BI / AI-focused Data Engineer

Spark · Databricks · Azure · Python
