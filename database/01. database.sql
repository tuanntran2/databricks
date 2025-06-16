-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create an empty database

-- COMMAND ----------

drop database if exists test_db;
CREATE DATABASE test_db;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Show & Describe database

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database test_db;

-- COMMAND ----------

describe database extended test_db;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Specify current database

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use test_db;
select current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Database Functions

-- COMMAND ----------

select concat(driver_ref, '-', code) AS new_driver_ref
from processed.drivers;

-- COMMAND ----------

select split(name, ' ')[0] as fore_name, split(name, ' ')[1] as sur_name
from processed.drivers;

-- COMMAND ----------

select current_timestamp
from processed.drivers

-- COMMAND ----------

select date_format(dob, 'dd-MM-yyyy')
from processed.drivers

-- COMMAND ----------

select dob, date_add(dob, 10)
from processed.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Drop database

-- COMMAND ----------

drop database test_db;
