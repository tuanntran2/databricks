# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops multi-line JSON file
# MAGIC - Read the pit_stops.json file from the raw container
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
        StructField("stop", IntegerType(), False),
        StructField("lap", IntegerType(), False),
        StructField("time", StringType(), False),
        StructField("duration", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
    ]
)

# COMMAND ----------

data = read_json("pit_stops.json", raw_mount_point, schema, multiline=True)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


data = to_snake_case(data) \
.withColumn(
    "ingestion_date", current_timestamp()
)

# COMMAND ----------

write_parquet(data, "pit_stops.parquet", processed_mount_point)

# COMMAND ----------

read_parquet("pit_stops.parquet", processed_mount_point).display()
