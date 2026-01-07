from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

df_carts =  spark.read.parquet("/Volumes/databricks_project_poc/filestore/ods/carts")
# df_products = spark.read.parquet("/FileStore/tables/products.parquet")


# =========================
# 1. Basic data quality checks
# =========================

print("Total records:", df_carts.count())

df_carts.select(
    col("id").isNull().alias("id_null"),
    col("userId").isNull().alias("user_null"),
    col("total").isNull().alias("total_null")
).show()

# =========================
# 2. Remove invalid records
# =========================
df_clean = (
    df_carts
    .filter(col("userId").isNotNull())
    .filter(col("total") >= 0)
)

# display(df_clean)

# =========================
# 3. Flatten nested products
# =========================

df_exploded = df_clean.selectExpr(
    "id as cart_id",
    "userId",
    "explode(products) as product",
    "total"
)

df_exploded = df_exploded.select(
    "cart_id",
    "userId",
    col("product.id").alias("product_id"),
    col("product.quantity").alias("quantity"),
    col("product.price").alias("price"),
    "total"
)

df_exploded.show(5)

# =========================
# 4. Save DWD layer
# =========================
df_exploded.write.mode("overwrite").parquet("/Volumes/databricks_project_poc/filestore/dwd/cart_items")
