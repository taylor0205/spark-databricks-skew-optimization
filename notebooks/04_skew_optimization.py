# Solutions for data skew issue（Salting + Broadcast）
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, concat_ws, split, sum as _sum, broadcast

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/Volumes/databricks_project_poc/filestore/dwd/cart_items")
df_products = spark.read.parquet("/Volumes/databricks_project_poc/filestore/ods/products")
# display(df)

# =========================
# 1. Salting for skewed userId
# =========================
df_salted = df.withColumn(
    "salted_user",
    concat_ws("_", df.userId, (rand() * 10).cast("int"))
)

df_partial = df_salted.groupBy("salted_user").agg(
    _sum("total").alias("partial_total")
)

df_final = (
    df_partial
    .withColumn("userId", split("salted_user", "_")[0])
    .groupBy("userId")
    .agg(_sum("partial_total").alias("total_spent"))
)

df_final.show(5)

# =========================
# 2. Broadcast join (small dimension)
# =========================
df_joined = df.join(
    broadcast(df_products),
    df.product_id == df_products.product_id,
    "left"
)
display(df_joined)
df_joined.select("userId", "brand", "category", df_products["price"]).show(5)

# =========================
# 3. Save optimized result
# =========================
df_final.write.mode("overwrite").parquet("/Volumes/databricks_project_poc/filestore/dws/user_spending")
