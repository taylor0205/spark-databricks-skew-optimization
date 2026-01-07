# The purpose is for performance monitoring and optimization effect verification

from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/Volumes/databricks_project_poc/filestore/dwd/cart_items")

# =========================
# 1. Measure execution time (before)
# =========================
start = time.time()
df.groupBy("userId").sum("total").count()
end = time.time()

print("Execution time (baseline):", end - start)

# =========================
# 2. Optimized execution
# =========================
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

start = time.time()
df.repartition("userId").groupBy("userId").sum("total").count()
end = time.time()

print("Execution time (optimized):", end - start)

# =========================
# 3. Spark UI (Manual Validation)
# =========================
# Compare:
# - Stage duration
# - Shuffle read/write
# - Max task time
