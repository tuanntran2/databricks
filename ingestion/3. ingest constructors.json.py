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
        StructField("constructorId", IntegerType(), True),
        StructField("constructorRef", StringType(), True),
        StructField("name", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

data = read_json("constructors.json", raw_mount_point, schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


data = to_snake_case(data) \
.withColumn("ingestion_date", current_timestamp()) \
.drop("url")

# COMMAND ----------

write_parquet(data, "constructors.parquet", processed_mount_point)

# COMMAND ----------

read_parquet("constructors.parquet", processed_mount_point).display()
