# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest orders JSON file

# COMMAND ----------

# MAGIC %run ../utils/utils

# COMMAND ----------

gizmobox_mount_point = mount_adls("gizmobox")


# COMMAND ----------

data = read_text("landing/operational_data/orders", gizmobox_mount_point)

# COMMAND ----------

data.write.mode("overwrite").saveAsTable(f"bronze.orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_fixed_temp_view
# MAGIC AS
# MAGIC SELECT
# MAGIC     -- This function search for "order_date" that is missing double quotes, and add the missing double quotes
# MAGIC     -- e.g. "order_date": 2025-01-01 ==> "order_date": "2025-01-01"
# MAGIC     regexp_replace(value, '"order_date": (\\d{4}-\\d{2}-\\d{2})', '"order_date": "\$1"') AS fixed_value
# MAGIC FROM bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_fixed_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the schema of the view
# MAGIC SELECT
# MAGIC     schema_of_json(fixed_value)
# MAGIC FROM orders_fixed_temp_view
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     from_json(
# MAGIC       fixed_value,
# MAGIC 'STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>'
# MAGIC     ) AS json_value
# MAGIC FROM orders_fixed_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.orders_json
# MAGIC AS
# MAGIC SELECT
# MAGIC     from_json(
# MAGIC       fixed_value,
# MAGIC 'STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>'
# MAGIC     ) AS json_value
# MAGIC FROM orders_fixed_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     json_value.order_id,
# MAGIC     json_value.order_status,
# MAGIC     json_value.order_status,
# MAGIC     json_value.payment_method,
# MAGIC     json_value.total_amount,
# MAGIC     json_value.transaction_timestamp,
# MAGIC     json_value.customer_id,
# MAGIC     json_value.items
# MAGIC FROM silver.orders_json;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     json_value.order_id,
# MAGIC     json_value.order_status,
# MAGIC     json_value.order_status,
# MAGIC     json_value.payment_method,
# MAGIC     json_value.total_amount,
# MAGIC     json_value.transaction_timestamp,
# MAGIC     json_value.customer_id,
# MAGIC     array_distinct(json_value.items) AS items
# MAGIC FROM silver.orders_json;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     json_value.order_id,
# MAGIC     json_value.order_status,
# MAGIC     json_value.payment_method,
# MAGIC     json_value.total_amount,
# MAGIC     json_value.transaction_timestamp,
# MAGIC     json_value.customer_id,
# MAGIC     explode(array_distinct(json_value.items)) AS item
# MAGIC FROM silver.orders_json;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_temp_view
# MAGIC AS
# MAGIC SELECT 
# MAGIC     json_value.order_id,
# MAGIC     json_value.order_status,
# MAGIC     json_value.payment_method,
# MAGIC     json_value.total_amount,
# MAGIC     json_value.transaction_timestamp,
# MAGIC     json_value.customer_id,
# MAGIC     explode(array_distinct(json_value.items)) AS item
# MAGIC FROM silver.orders_json;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   order_status
# MAGIC   payment_method,
# MAGIC   total_amount,
# MAGIC   transaction_timestamp,
# MAGIC   customer_id,
# MAGIC   item.category,
# MAGIC   item.details.brand,
# MAGIC   item.details.color,
# MAGIC   item.item_id,
# MAGIC   item.name,
# MAGIC   item.price,
# MAGIC   item.quantity
# MAGIC FROM orders_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.orders
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   order_status
# MAGIC   payment_method,
# MAGIC   total_amount,
# MAGIC   transaction_timestamp,
# MAGIC   customer_id,
# MAGIC   item.category,
# MAGIC   item.details.brand,
# MAGIC   item.details.color,
# MAGIC   item.item_id,
# MAGIC   item.name,
# MAGIC   item.price,
# MAGIC   item.quantity
# MAGIC FROM orders_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.orders;
