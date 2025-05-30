# Databricks notebook source
# MAGIC %run ../utils/utils

# COMMAND ----------

raw_mount_point = mount_adls("raw")
processed_mount_point = mount_adls("processed")

# COMMAND ----------

# schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

from pyspark.sql.types import StructField, IntegerType, StringType


schema = StructType(
    fields = [
        StructField("qualifyingId", IntegerType(), False),
        StructField("raceId", IntegerType(), True),
        StructField("driverId", IntegerType(), True),
        StructField("constructorId", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("q1", StringType(), True),
        StructField("q2", StringType(), True),
        StructField("q3", StringType(), True),
    ]
)


# COMMAND ----------

data = read_json("qualifying", raw_mount_point, schema, multiline=True)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


data = to_snake_case(data) \
.withColumn(
    "ingestion_date", current_timestamp()
)

# COMMAND ----------

write_parquet(data, "qualifying.parquet", processed_mount_point)

# COMMAND ----------

read_parquet("qualifying.parquet", processed_mount_point).display()
