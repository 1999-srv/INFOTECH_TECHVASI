# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, DecimalType
from pyspark.sql.functions import col, from_utc_timestamp, to_utc_timestamp, dayofyear, when, lit
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

# Convert &#39;ModifiedDate&#39; to timestamp
df = df.withColumn("ModifiedDate",col("ModifiedDate").cast("timestamp"))

# COMMAND ----------

# Create the time zone table
time_zone_table = [
    {"zone_id": "Europe/London", "local_time_with_dst": "BST", "local_time_without_dst": "GMT"},
    {"zone_id": "Europe/Berlin", "local_time_with_dst": "CEST", "local_time_without_dst": "CET"},
    {"zone_id": "Europe/Paris", "local_time_with_dst": "CEST", "local_time_without_dst": "CET"},
    {"zone_id": "Europe/Madrid", "local_time_with_dst": "CEST", "local_time_without_dst": "CET"},
    {"zone_id": "Europe/Rome", "local_time_with_dst": "CEST", "local_time_without_dst": "CET"}
    # Add other relevant zones here
]

# Create a DataFrame for the time zones
zone_df = spark.createDataFrame(time_zone_table)
# zone_df.show()

# Convert 'ModifiedDate' to UTC
df_with_timezone = df.withColumn("UTC", to_utc_timestamp(col("ModifiedDate"), "UTC"))

# Add Local_Time_Without_DST column using 'Europe/London' (default to GMT)
df_with_timezone = df_with_timezone.withColumn(
    "Local_Time_Without_DST", from_utc_timestamp(col("UTC"), "Europe/London")
)

# Add Local_Time_With_DST column using 'Europe/Berlin' (default to CEST)
df_with_timezone = df_with_timezone.withColumn(
    "Local_Time_With_DST", from_utc_timestamp(col("UTC"), "Europe/Berlin")
)

# Calculate Day of the Year for DST logic
df_with_timezone = df_with_timezone.withColumn("Day_Of_Year", dayofyear(col("ModifiedDate")))

# Determine if DST is on or off (assuming DST is between day 60 and day 300 of the year for Europe)
df_with_timezone = df_with_timezone.withColumn(
    "Is_DST_On", when((col("Day_Of_Year") >= 60) & (col("Day_Of_Year") <= 300), lit("Yes")).otherwise(lit("No"))
)

# Show the final DataFrame with UTC, local times, and DST status
df_with_timezone.select("ModifiedDate", "UTC", "Local_Time_Without_DST", "Local_Time_With_DST", "Day_Of_Year", "Is_DST_On").show(truncate=False)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import functions as F

# Start Spark session
spark = SparkSession.builder.appName("SalesOrderDetail").getOrCreate()

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

# Load the CSV file with the defined schema
file_path = '/FileStore/tables/Sales_SalesOrderDetail__2_.csv'
df = spark.read.csv(file_path, header=True, schema=schema)

# Add UTC and various timezones
df = df.withColumn("UTC", to_timestamp(col("ModifiedDate"), "yyyy-MM-dd"))

# Define various time zones
time_zones = {
    "IST": "Asia/Kolkata",
    "GMT/WET": "Europe/London",
    "CET": "Europe/Berlin",
    "EET": "Europe/Kiev"
}

# Add columns with converted times
for tz_name, tz in time_zones.items():
    df = df.withColumn(tz_name, F.from_utc_timestamp(col("UTC"), tz))

# Show the result with the new columns
df.select("ModifiedDate", "UTC", "IST", "GMT/WET", "CET", "EET").show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType
from pyspark.sql.functions import col, to_timestamp, from_utc_timestamp, year, month, concat, lit


# Initialize Spark session
spark = SparkSession.builder.appName("SalesOrderDetailProcessing").getOrCreate()

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

# Load the CSV file with the defined schema
file_path = '/FileStore/tables/Sales_SalesOrderDetail__2_.csv'
df = spark.read.csv(file_path, header=True, schema=schema)

# Convert ModifiedDate into timestamp
df = df.withColumn("UTC", to_timestamp(col("ModifiedDate"), "yyyy-MM-dd"))

# Add columns for 5 European time zones with DST handling
df = df.withColumn("London_GMT/WET", from_utc_timestamp(col("UTC"), "Europe/London")) \
       .withColumn("Paris_CET", from_utc_timestamp(col("UTC"), "Europe/Paris")) \
       .withColumn("Helsinki_EET", from_utc_timestamp(col("UTC"), "Europe/Helsinki")) \
       .withColumn("Moscow_MSK", from_utc_timestamp(col("UTC"), "Europe/Moscow")) \
       .withColumn("Lisbon_WEST", from_utc_timestamp(col("UTC"), "Europe/Lisbon"))

# Extract year and month from ModifiedDate
df = df.withColumn("Year", year(col("ModifiedDate"))) \
       .withColumn("Month", month(col("ModifiedDate")))

# Combine Year and Month into a single column for easier partitioning
df = df.withColumn("YearMonth", concat(col("Year"), lit("_"), col("Month")))

# Show the resulting DataFrame with year and month
df.select("ModifiedDate", "Year", "Month", "YearMonth", "UTC", "London_GMT/WET", "Paris_CET", "Helsinki_EET", "Moscow_MSK", "Lisbon_WEST").show()

# Divide the data by year and month and save into different files based on Year and Month
year_months = df.select("YearMonth").distinct().rdd.flatMap(lambda x: x).collect()

for ym in year_months:
    df_ym = df.filter(col("YearMonth") == ym)
    # Save each year and month's data into separate files
    df_ym.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(f"default.sales_data_{ym}", header=True)
