# Databricks notebook source
# MAGIC %md
# MAGIC ### Create secret scope
# MAGIC - On the Databricks home page, append "**_#secrets/createScope_**" to the URL
# MAGIC - Specify scope name
# MAGIC - Navigate to the Settings/Properties page of the created Azure Key vault
# MAGIC - Copy the Vault URI value into the scope DNS Name field
# MAGIC - Copy the Resource ID value into the scope Resource ID field

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("secret_scope")

# COMMAND ----------

dbutils.secrets.get("secret_scope", "secret_key")
