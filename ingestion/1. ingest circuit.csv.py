# Databricks notebook source
# MAGIC %run ../utils/utils

# COMMAND ----------

raw_mount_point = mount_adls("raw")
processed_mount_point = mount_adls("processed")

# COMMAND ----------

from pyspark.sql.types import StructField, IntegerType, StringType, DoubleType


schema = StructType(
    fields = [
        StructField("circuitId", IntegerType(), False),
        StructField("circuitRef", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("alt", IntegerType(), True)
    ]
)

# COMMAND ----------

data = read_csv("circuits.csv", raw_mount_point, schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


data = to_snake_case(data) \
.withColumnsRenamed(
    {
        "lat": "latitude",
        "lng": "longitude",
        "alt": "altitude",
    }
).withColumn(
    "ingestion_date",
    current_timestamp()
).drop("url")

# COMMAND ----------

write_parquet(data, "circuits.parquet", processed_mount_point)

# COMMAND ----------

read_parquet("circuits.parquet", processed_mount_point).display()
