-- Databricks notebook source
-- MAGIC %md
-- MAGIC # External Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create External Tables using Python

-- COMMAND ----------

-- MAGIC %run ../utils/utils

-- COMMAND ----------

-- MAGIC %python
-- MAGIC presentation_mount_point = mount_adls("presentation")
-- MAGIC demo_mount_point = mount_adls("demo")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = read_parquet("race_results.parquet", presentation_mount_point)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
-- MAGIC
-- MAGIC # to create an external table, we need to specify the path for the external table
-- MAGIC data.write.format("parquet") \
-- MAGIC     .option("path", f"{demo_mount_point}/race_results_ext_py") \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create External Table using SQL

-- COMMAND ----------

select *
from demo.race_results_ext_py;

-- COMMAND ----------

DROP TABLE IF EXISTS  demo.race_results_ext_sql;

CREATE TABLE demo.race_results_ext_sql(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name string,
  driver_number INT,
  driver_nationality STRING,
  team string,
  grid INT,
  fastest_lap INT,
  race_time string,
  points FLOAT,
  position INT,
  file_date STRING,
  created_date TIMESTAMP,
  race_id INT
)
USING parquet
LOCATION "/mnt/demo/race_results_ext_sql"

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_sql;

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_sql;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py
WHERE race_year = 2020;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

select count(1) from demo.race_results_sql;

-- COMMAND ----------

drop table  demo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC read_parquet("race_results_ext_sql", demo_mount_point).display()

-- COMMAND ----------

select * from  demo.race_results_ext_sql;
