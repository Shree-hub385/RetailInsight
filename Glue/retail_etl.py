
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, trim
)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("RetailInsight_ETL") \
    .getOrCreate()

# S3 input and output locations
INPUT_PATH = "s3://ReatailInsight-bucket/raw/retail_sales_dataset.csv"
OUTPUT_PATH = "s3://ReatailInsight-bucket/processed/retail_sales/"

# Read retail sales CSV
df = spark.read.csv(
    INPUT_PATH,
    header=True,
    inferSchema=True
)

# Remove duplicate transactions
df = df.dropDuplicates(["Transaction ID"])

# Clean column values
df = df.withColumn(
    "Gender", trim(col("Gender"))
).withColumn(
    "Product Category", trim(col("Product Category"))
).withColumn(
    "Customer ID", trim(col("Customer ID"))
)

# Convert date into proper date format
df = df.withColumn(
    "Date", to_date(col("Date"), "dd-MM-yy")
)

# Remove invalid records
df = df.filter(
    col("Transaction ID").isNotNull() &
    col("Customer ID").isNotNull() &
    col("Date").isNotNull() &
    (col("Quantity") > 0) &
    (col("Price per Unit") >= 0) &
    (col("Total Amount") >= 0)
)

# Add year and month columns
df = df.withColumn("Year", year(col("Date")))
df = df.withColumn("Month", month(col("Date")))

# Display processed data
df.show(10, truncate=False)

# Display total record count
print("Processed Records:", df.count())

# Save transformed data as Parquet
df.write.mode("overwrite").parquet(OUTPUT_PATH)

print("RetailInsight ETL processing completed!")

spark.stop()
