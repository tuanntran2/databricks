# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest orders JSON file

# COMMAND ----------

# MAGIC %run ../utils/utils

# COMMAND ----------

gizmobox_mount_point = mount_adls("gizmobox")


# COMMAND ----------

data = read_png("landing/operational_data/memberships/*/*.png", gizmobox_mount_point)
data.display()

# COMMAND ----------

data.write.mode("overwrite").saveAsTable(f"bronze.memberships")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     regexp_extract(path, r".*\/(\d+)\.png$", 1) AS customer_id,
# MAGIC     content AS membership_card
# MAGIC FROM bronze.memberships;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.memberships
# MAGIC AS
# MAGIC SELECT
# MAGIC     regexp_extract(path, r".*\/(\d+)\.png$", 1) AS customer_id,
# MAGIC     content AS membership_card
# MAGIC FROM bronze.memberships;
