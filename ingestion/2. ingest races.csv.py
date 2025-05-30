# Databricks notebook source
# MAGIC %run ../utils/utils

# COMMAND ----------

raw_mount_point = mount_adls("raw")
processed_mount_point = mount_adls("processed")

# COMMAND ----------

from pyspark.sql.types import StructField, IntegerType, StringType, DateType


schema = StructType(
    fields = [
        StructField("raceId", IntegerType(), False),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("time", StringType(), True),
    ]
)

# COMMAND ----------

data = read_csv("races.csv", raw_mount_point, schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit, to_timestamp, col


datetime_string = concat(data.date, lit(" "), data.time)
datetime_value = to_timestamp(datetime_string, "yyyy-MM-dd HH:mm:ss")
data = data.withColumn("race_timestamp", datetime_value)
data = data.withColumn("ingestion_date", current_timestamp())

data = data.select(
    col("raceId").alias("race_id"),
    col("year").alias("race_year"),
    col("round"),
    col("circuitId").alias("circuit_id"),
    col("name"),
    col("race_timestamp"),
    col("ingestion_date"),
)

# COMMAND ----------

write_parquet(data, "races.parquet", processed_mount_point)

# COMMAND ----------

read_parquet("races.parquet", processed_mount_point).display()
