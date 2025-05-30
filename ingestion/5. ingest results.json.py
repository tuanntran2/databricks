# Databricks notebook source
# MAGIC %run ../utils/utils

# COMMAND ----------

raw_mount_point = mount_adls("raw")
processed_mount_point = mount_adls("processed")

# COMMAND ----------

from pyspark.sql.types import StructField, IntegerType, StringType, FloatType


schema = StructType(
    fields = [
        StructField("resultId", IntegerType(), False),
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), False),
        StructField("constructorId", IntegerType(), False),
        StructField("number", IntegerType(), True),
        StructField("grid", IntegerType(), False),
        StructField("position", IntegerType(), True),
        StructField("positionText", StringType(), False),
        StructField("positionOrder", IntegerType(), False),
        StructField("points", FloatType(), False),
        StructField("laps", IntegerType(), False),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
        StructField("fastestLap", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("fastestLapSpeed", StringType(), True),
        StructField("statusId", IntegerType(), False),
    ]
)

# COMMAND ----------

data = read_json("results.json", raw_mount_point, schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


data = to_snake_case(data) \
.withColumn(
    "ingestion_date", current_timestamp()
).drop("statusId")

# COMMAND ----------

write_parquet(data, "results.parquet", processed_mount_point)

# COMMAND ----------

read_parquet("results.parquet", processed_mount_point).display()
