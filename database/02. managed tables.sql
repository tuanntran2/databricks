-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Managed Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Managed Tables using Python

-- COMMAND ----------

-- MAGIC %run ../utils/utils

-- COMMAND ----------

-- MAGIC %python
-- MAGIC presentation_mount_point = mount_adls("presentation")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = read_parquet("race_results.parquet", presentation_mount_point)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data.write.format("parquet").saveAsTable("demo.race_results_py")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Managed Tables using SQL

-- COMMAND ----------

select *
from demo.race_results_py;

-- COMMAND ----------

create table demo.race_results_sql
as
select * from presentation.race_results where race_year = 2020;

-- COMMAND ----------

select * from demo.race_results_sql;

-- COMMAND ----------

show tables in demo;
