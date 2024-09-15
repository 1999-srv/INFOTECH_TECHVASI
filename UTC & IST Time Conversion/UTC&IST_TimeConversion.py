# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_utc_timestamp, from_utc_timestamp
from pyspark.sql.types import StringType, StructType, StructField
import datetime

# Create Spark session
spark = SparkSession.builder.appName("TimeConversion").getOrCreate()

# Sample dataset using for loop
data = [(datetime.datetime(2024, 9, 6, 12, 30) + datetime.timedelta(hours=i)).strftime('%Y-%m-%d %H:%M:%S') for i in range(35)]
schema = StructType([StructField("date_col", StringType(), True)])

# Create dataFrame using for loop
df = spark.createDataFrame([(d,) for d in data], schema)

# Show the dataFrame
df.show()

# Convert date_col to UTC
df = df.withColumn("UTC_time", to_utc_timestamp(col("date_col"), "US/Pacific"))

# Convert UTC to IST (IST is UTC+5:30)
df = df.withColumn("IST_time", from_utc_timestamp(col("UTC_time"), "Asia/Kolkata"))

# Show the results
df.show(truncate=False)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

# Define the schema
schema = StructType([
    StructField("SalesOrderID", IntegerType(), True),
    StructField("SalesOrderDetailID", IntegerType(), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    StructField("OrderQty", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("SpecialOfferID", IntegerType(), True),
    StructField("UnitPrice", DecimalType(10, 2), True),
    StructField("UnitPriceDiscount", DecimalType(10, 2), True),
    StructField("LineTotal", DecimalType(20, 2), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", DateType(), True)
])

# Load the data with the schema
df = spark.read.csv("/FileStore/tables/Sales_SalesOrderDetail__2_.csv", schema=schema, header=False)

# Show the DataFrame
df.show()

from pyspark.sql.functions import from_utc_timestamp, to_utc_timestamp

# Load the data with the schema
df = spark.read.csv("/FileStore/tables/Sales_SalesOrderDetail__2_.csv", schema=schema, header=False)

# Add two extra columns for IST and UTC timestamps
df_with_timezones = df.withColumn(
    "IST_Time", from_utc_timestamp("ModifiedDate", "Asia/Kolkata")
).withColumn(
    "UTC_Time", to_utc_timestamp("ModifiedDate", "UTC")
)

# Show the DataFrame with the new columns for IST and UTC
df_with_timezones.select("ModifiedDate", "IST_Time", "UTC_Time").show(truncate=False)