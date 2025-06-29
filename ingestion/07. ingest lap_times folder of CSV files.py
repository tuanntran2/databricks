# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest all the CSV files from the lap_times folder
# MAGIC - Read all CSV files in lap_times folder from the raw container into 1 dataframe
# MAGIC - Rename some columns (convert pascal casing to snake casing)
# MAGIC - Add ingestion_date column with the current timestamp
# MAGIC - Save the data as parquet file to the processed container

# COMMAND ----------

# MAGIC %run ../utils/utils

# COMMAND ----------

raw_mount_point = mount_adls("raw")
processed_mount_point = mount_adls("processed")

# COMMAND ----------

from pyspark.sql.types import StructField, IntegerType, StringType


schema = StructType(
    fields = [
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), False),
        StructField("lap", IntegerType(), False),
        StructField("position", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
    ]
)

# COMMAND ----------

data = read_csv("lap_times", raw_mount_point, schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


data = to_snake_case(data) \
.withColumn(
    "ingestion_date", current_timestamp()
)

# COMMAND ----------

write_parquet(data, "lap_times.parquet", processed_mount_point)

# COMMAND ----------

read_parquet("lap_times.parquet", processed_mount_point).display()
