# Databricks notebook source
# MAGIC %md
# MAGIC # Read/Write Data Utilities

# COMMAND ----------

# MAGIC %run ./configuration

# COMMAND ----------

import pyspark.sql.dataframe as spark_dataframe
from pyspark.sql.types import StructType

# COMMAND ----------

def read_text(
    file_name: str,
    mount_point: str
) -> spark_dataframe:
    return spark.read.text(
        f"{mount_point}/{file_name}"
    )

# COMMAND ----------

def read_csv(
    file_name: str,
    mount_point: str,
    schema: StructType=None,
) -> spark_dataframe:

    return spark.read.csv(
        f"{mount_point}/{file_name}",
        header=True,
        schema=schema,
    )

# COMMAND ----------

def read_tsv(
    file_name: str,
    mount_point: str,
    schema: StructType=None,
) -> spark_dataframe:

    return spark.read.csv(
        f"{mount_point}/{file_name}",
        header=True,
        schema=schema,
        sep='\t',
    )

# COMMAND ----------

def read_json(
    file_name: str,
    mount_point: str,
    schema: StructType=None,
    multiline: bool=False,
) -> spark_dataframe:

    return spark.read.option("multiline", multiline).json(
        f"{mount_point}/{file_name}",
        schema=schema,
    )

# COMMAND ----------

def read_png(
    file_name: str,
    mount_point: str,
) -> spark_dataframe:

    return spark.read.format("binaryFile") \
        .option("mimeType", "image/png") \
        .load(f"{mount_point}/{file_name}")

# COMMAND ----------

def read_parquet(
    file_name: str,
    mount_point: str,
) -> spark_dataframe:

    return spark.read.parquet(
        f"{mount_point}/{file_name}",
    )

# COMMAND ----------

def write_parquet(
    data: spark_dataframe,
    file_name: str,
    mount_point: str,
    mode: str="overwrite",
    partition_by: str = None,
) -> spark_dataframe:

    if mode == "update":
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        data = move_column_to_last(data, partition_by)
        mode = "overwrite"

    partition_by = [partition_by] if partition_by else []

    return data.write.partitionBy(*tuple(partition_by)) \
        .parquet(f"{mount_point}/{file_name}", mode=mode)

# COMMAND ----------

def upsert_table_old(
    data: spark_dataframe,
    table_name: str,
    mode: str="overwrite",
    partition_by: str = None,
):
    if mode == "overwrite":
        if partition_by:
            data.write.mode("overwrite").partitionBy(partition_by).saveAsTable(f"{table_name}")
        else:
            data.write.mode("overwrite").saveAsTable(f"{table_name}")
    elif mode == "update":
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        data = move_column_to_last(data, partition_by)

        if (spark._jsparkSession.catalog().tableExists(f"{table_name}")):
            data.write.mode("overwrite").insertInto(f"{table_name}")
        else:
            data.write.mode("overwrite").partitionBy(partition_by).saveAsTable(f"{table_name}")
    else:
        raise ValueError(f"Invalid mode: {mode}")

# COMMAND ----------

from delta.tables import DeltaTable


def upsert_table(
    data: spark_dataframe,
    table_name: str,
    mode: str="overwrite",
    partition_by: str = None,
    conditions: str = None,
):
    if mode == "overwrite":
        if partition_by:
            data.write.mode("overwrite").partitionBy(partition_by).saveAsTable(f"{table_name}")
        else:
            data.write.mode("overwrite").saveAsTable(f"{table_name}")
    elif mode == "update":
        if (spark._jsparkSession.catalog().tableExists(f"{table_name}")):
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
            delta_table = DeltaTable.forName(spark, table_name)
            delta_table.alias("tgt").merge(
                data.alias("src"),
                conditions,
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        else:
            data.write.mode("overwrite").partitionBy(partition_by).saveAsTable(f"{table_name}")
    else:
        raise ValueError(f"Invalid mode: {mode}")

# COMMAND ----------

def read_stream(
    file_name: str,
    mount_point: str,
    schema: StructType=None,
    fmt: str="json"
) -> spark_dataframe:

    return spark.readStream \
        .format(fmt) \
        .schema(schema) \
        .load(f"{mount_point}/{file_name}")

# COMMAND ----------

"""
output_mode:
    "append"   - Write new rows since last micro-batch
    "complete" - Write entire result
    "update"   - Write only updated rows since last micro-batch
"""

def write_stream(
    data: spark_dataframe,
    table_name: str,
    checkpoint: str = None,
    output_mode: str = "append"
):
    writer = data.writeStream.format("delta")
    if checkpoint:
        writer = writer.option("checkpointLocation", checkpoint)
    return writer.toTable(table_name)
