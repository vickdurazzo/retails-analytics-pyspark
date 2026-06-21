from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, row_number, when, upper, trim
)


spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .getOrCreate()

bronze_df = spark.read.parquet(
    "/opt/spark-data/bronze/retail_sales_bronze.parquet"
)

#print("Bronze count:", bronze_df.count())

#----------------
# Deduplicação
#----------------
#bronze_df.groupBy("transaction_id","order_date").count().filter(col("count") > 1).show(5, truncate=False)

# Manter apenas o registro com a menor order_date por transaction_id
window_spec = Window.partitionBy("transaction_id").orderBy(col("order_date").asc())
bronze_with_row = bronze_df.withColumn("rn", row_number().over(window_spec))
silver_df = bronze_with_row.filter(col("rn")==1).drop("rn")

#print("Silver count:", silver_df.count())

#-------------------
# Correção das datas
#-------------------
#bronze_df.filter(col("ship_date")< col("order_date")).show(5)

silver_df = silver_df.withColumn(
    "ship_date",
    when(col("ship_date")<col("order_date"),None)\
    .otherwise(col("ship_date"))
)

#----------------
# Limpeza das col. de quantidade e preço
#----------------

#silver_df.filter(col("quantity")<=0).show(5)

silver_df = silver_df.filter(col("quantity")>0)

#silver_df.filter(col("unit_price")<= 0).show(5)

silver_df = silver_df.withColumn(
    "unit_price",
    when(col("unit_price")<=0,None)\
    .otherwise(col("unit_price"))
)

#---------------
# Limpeza do Desconto
#---------------

#silver_df.filter(
#    (col("discount_pct")<0) | (col("discount_pct") > 100)
#).show(5)

silver_df = silver_df.withColumn(
    "discount_pct",
    when((col("discount_pct")<0) | (col("discount_pct") > 100),None)
    .otherwise(col("discount_pct"))
)

# ----------------
# Normalizacao Genero
#------------------

#silver_df.groupby("gender").count().show()

silver_df = silver_df.withColumn(
    "gender",
    when(upper(trim(col("gender")))=="FEMALE","F")
    .when(upper(trim(col("gender")))=="MALE","M")
    .when(col("gender").isin("M","F"),col("gender"))
    .otherwise(None)
)

#-----------------
# Normalizacao Formas de pagamento
#------------------

#silver_df.groupby("payment_type").count().show()

silver_df = silver_df.withColumn(
    "payment_type",
    when(col("payment_type").isin("Card","UPI","COD"),col("payment_type"))
    .otherwise(None)
)

(
    silver_df
    .write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet("/opt/spark-data/bronze/retail_sales_silver.parquet")
)

print("✅ Silver layer created successfully")