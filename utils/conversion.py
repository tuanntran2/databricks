# Databricks notebook source
# MAGIC %md
# MAGIC # Conversion Utilities
# MAGIC - Function to convert a string in pascal case to snake case
# MAGIC - Function to convert spark dataframe columns from pascal case to snake case
# MAGIC - Function to move a column in a spark dataframe to be the last column

# COMMAND ----------

import re
def camel_case_to_snake_case(value: str) -> str:
    return re.sub(r'([a-z])([A-Z])', r'\1_\2', value).lower()

# COMMAND ----------

import pyspark.sql.dataframe as spark_dataframe
from pyspark.sql.functions import col


def to_snake_case(spark_df: spark_dataframe) -> spark_dataframe:
    return spark_df.select(
        [
            col(column).alias(camel_case_to_snake_case(column))
            for column in spark_df.columns
        ]
    )

# COMMAND ----------

def move_column_to_last(spark_df, column_name):
    columns = [col for col in spark_df.columns if col != column_name] + [column_name]
    return spark_df.select(columns)
