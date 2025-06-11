# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest orders JSON file

# COMMAND ----------

# MAGIC %run ../utils/utils

# COMMAND ----------

gizmobox_mount_point = mount_adls("gizmobox")

# COMMAND ----------

data = read_json("landing/operational_data/customers", gizmobox_mount_point)

# COMMAND ----------

data.write.mode("overwrite").saveAsTable(f"bronze.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW bronze.customers_view
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   _metadata.file_path as file_path
# MAGIC FROM bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT
# MAGIC       customer_id,
# MAGIC       MAX(created_timestamp) as max_created_timestamp
# MAGIC   FROM bronze.customers
# MAGIC   WHERE customer_id IS NOT NULL
# MAGIC   GROUP BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte_max AS
# MAGIC (
# MAGIC   SELECT
# MAGIC       customer_id,
# MAGIC       MAX(created_timestamp) as max_created_timestamp
# MAGIC   FROM bronze.customers
# MAGIC   WHERE customer_id IS NOT NULL
# MAGIC   GROUP BY customer_id
# MAGIC )
# MAGIC SELECT DISTINCT *
# MAGIC FROM bronze.customers t
# MAGIC JOIN cte_max m
# MAGIC     ON t.customer_id = m.customer_id
# MAGIC     AND t.created_timestamp = m.max_created_timestamp
# MAGIC ORDER BY t.customer_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.customers
# MAGIC AS
# MAGIC WITH cte_max AS
# MAGIC (
# MAGIC   SELECT
# MAGIC       customer_id,
# MAGIC       MAX(created_timestamp) as max_created_timestamp
# MAGIC   FROM bronze.customers
# MAGIC   WHERE customer_id IS NOT NULL
# MAGIC   GROUP BY customer_id
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC     CAST(t.created_timestamp AS TIMESTAMP) AS max_created_timestamp,
# MAGIC     t.customer_id,
# MAGIC     t.customer_name,
# MAGIC     CAST(t.date_of_birth AS DATE) as date_of_birth,
# MAGIC     t.email,
# MAGIC     CAST(t.member_since AS DATE) AS member_since,
# MAGIC     t.telephone
# MAGIC FROM bronze.customers t
# MAGIC JOIN cte_max m
# MAGIC     ON t.customer_id = m.customer_id
# MAGIC     AND t.created_timestamp = m.max_created_timestamp
# MAGIC ORDER BY t.customer_id;
