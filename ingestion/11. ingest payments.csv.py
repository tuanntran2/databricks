# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest orders JSON file

# COMMAND ----------

# MAGIC %run ../utils/utils

# COMMAND ----------

gizmobox_mount_point = mount_adls("gizmobox")


# COMMAND ----------

from pyspark.sql.types import StructField, IntegerType, StringType, TimestampType


schema = StructType(
    fields = [
        StructField("payment_id", IntegerType(), False),
        StructField("order_id", IntegerType(), False),
        StructField("payment_timestamp", TimestampType(), False),
        StructField("payment_status", IntegerType(), False),
        StructField("payment_method", StringType(), False),
    ]
)

# COMMAND ----------

data = read_csv("landing/external_data/payments", gizmobox_mount_point, schema)

# COMMAND ----------

data.write.mode("overwrite").saveAsTable(f"bronze.payments")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     payment_id,
# MAGIC     order_id,
# MAGIC     CAST(date_format(payment_timestamp, 'yyyy-MM-dd') AS DATE) AS payment_date,
# MAGIC     date_format(payment_timestamp, 'HH:mm:ss') as payment_time,
# MAGIC     CASE payment_status
# MAGIC         WHEN 1 THEN 'Success'
# MAGIC         WHEN 2 THEN 'Pending'
# MAGIC         WHEN 3 THEN 'Cancelled'
# MAGIC         WHEN 4 THEN 'Failed'
# MAGIC     END AS payment_status,
# MAGIC     payment_method
# MAGIC FROM bronze.payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.payments
# MAGIC AS
# MAGIC SELECT
# MAGIC     payment_id,
# MAGIC     order_id,
# MAGIC     CAST(date_format(payment_timestamp, 'yyyy-MM-dd') AS DATE) AS payment_date,
# MAGIC     date_format(payment_timestamp, 'HH:mm:ss') as payment_time,
# MAGIC     CASE payment_status
# MAGIC         WHEN 1 THEN 'Success'
# MAGIC         WHEN 2 THEN 'Pending'
# MAGIC         WHEN 3 THEN 'Cancelled'
# MAGIC         WHEN 4 THEN 'Failed'
# MAGIC     END AS payment_status,
# MAGIC     payment_method
# MAGIC FROM bronze.payments;
