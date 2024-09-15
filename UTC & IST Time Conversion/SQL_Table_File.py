# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType

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

# Load the CSV with the specified schema
df = spark.read.csv("/FileStore/tables/Sales_SalesOrderDetail-1.csv", schema=schema, header=True)

# Write the DataFrame as a SQL table
df.write.format("delta").saveAsTable("hive_metastore.default.SalesOrderDetails")
