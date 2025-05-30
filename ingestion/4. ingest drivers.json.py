# Databricks notebook source
# MAGIC %run ../utils/utils

# COMMAND ----------

raw_mount_point = mount_adls("raw")
processed_mount_point = mount_adls("processed")

# COMMAND ----------

from pyspark.sql.types import StructField, IntegerType, StringType, DateType


NameType = StructType(
    fields = [
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True),
    ]
)

schema = StructType(
    fields = [
        StructField("driverId", IntegerType(), False),
        StructField("driverRef", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", NameType, True),
        StructField("dob", DateType(), True),
        StructField("nationality", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

data = read_json("drivers.json", raw_mount_point, schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit


data = to_snake_case(data) \
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumn("ingestion_date", current_timestamp()) \
.drop("url")

# COMMAND ----------

write_parquet(data, "drivers.parquet", processed_mount_point)

# COMMAND ----------

read_parquet("drivers.parquet", processed_mount_point).display()
