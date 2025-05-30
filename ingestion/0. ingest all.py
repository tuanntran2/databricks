# Databricks notebook source
# MAGIC %md
# MAGIC # Execute all notebooks in the current folder

# COMMAND ----------

from pathlib import Path


[
    dbutils.notebook.run(entry.stem, 0)
    for entry in sorted(Path().iterdir())
    if entry.is_file() and entry.stem != '0. ingest all'
]
