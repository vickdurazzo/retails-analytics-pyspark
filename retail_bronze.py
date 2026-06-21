from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
    DoubleType, DateType
)

spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .getOrCreate()

bronze_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("ship_date", DateType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("ingestion_date", DateType(), True)
])

bronze_df = (
    spark.read\
    .option("header", "true")\
    .schema(bronze_schema)\
    .csv("/opt/spark/work-dir/data/raw/retail_sales_raw.csv")
)

print("Record count:", bronze_df.count())
bronze_df.printSchema()

bronze_df\
    .write\
    .mode("overwrite")\
    .parquet("/opt/spark-data/bronze/retail_sales_bronze.parquet")