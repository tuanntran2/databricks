# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest orders JSON file

# COMMAND ----------

# MAGIC %run ../utils/utils

# COMMAND ----------

gizmobox_mount_point = mount_adls("gizmobox")


# COMMAND ----------

data = read_tsv(
    "landing/operational_data/addresses",
    gizmobox_mount_point,
)

# COMMAND ----------

data.write.mode("overwrite").saveAsTable(f"bronze.addresses")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     address_type,
# MAGIC     address_line_1,
# MAGIC     city,
# MAGIC     state,
# MAGIC     postcode
# MAGIC FROM bronze.addresses;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         address_type,
# MAGIC         address_line_1,
# MAGIC         city,
# MAGIC         state,
# MAGIC         postcode
# MAGIC     FROM bronze.addresses
# MAGIC )
# MAGIC PIVOT (
# MAGIC     MAX(address_line_1) AS address_line_1,
# MAGIC     MAX(city) AS city,
# MAGIC     MAX(state) AS state,
# MAGIC     MAX(postcode) AS postcode
# MAGIC     FOR address_type IN ('shipping', 'billing')
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.addresses
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         address_type,
# MAGIC         address_line_1,
# MAGIC         city,
# MAGIC         state,
# MAGIC         postcode
# MAGIC     FROM bronze.addresses
# MAGIC )
# MAGIC PIVOT (
# MAGIC     MAX(address_line_1) AS address_line_1,
# MAGIC     MAX(city) AS city,
# MAGIC     MAX(state) AS state,
# MAGIC     MAX(postcode) AS postcode
# MAGIC     FOR address_type IN ('shipping', 'billing')
# MAGIC );
