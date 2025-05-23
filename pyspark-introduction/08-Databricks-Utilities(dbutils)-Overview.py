# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Goal of this Notebook
# MAGIC  In this lesson, we learn how to use databrick utilities: dbutils. we use the fs command which allows us to access the databricks filesystem from our notebook and do a simple ls on one of the directory paths in our storage account to list all the files in that directory path
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC CSV Source File Path : "abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv"
# MAGIC
# MAGIC JSON File Path : "abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json"
# MAGIC
# MAGIC PARQUET Target  File Path : "abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/parquet"
# MAGIC
# MAGIC
# MAGIC ##### Databricks Utilities
# MAGIC - <a href="https://docs.databricks.com/en/dev-tools/databricks-utils.html">dbutils</a>

# COMMAND ----------

storageAccountKey=''
spark.conf.set("fs.azure.account.key.dbrkcourse2025storagedev.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

sourceCSVFilePath='abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv'
sourceJSONFilePath='abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json'
sourcePARQUETFilePath='abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/parquet'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()


# COMMAND ----------

dbutils.fs.help("ls")


# COMMAND ----------

dbutils.fs.ls(sourceCSVFilePath)
