# Databricks notebook source
# MAGIC %md
# MAGIC ##### Source File Details
# MAGIC Source File URL : "https://retailpricing.blob.core.windows.net/labs/lab1/PW_MW_DR_01012023.csv"
# MAGIC
# MAGIC Source File Ingestion Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC ##### Python Core Library Documentation
# MAGIC - <a href="https://pandas.pydata.org/docs/user_guide/index.html#user-guide" target="_blank">pandas</a>
# MAGIC - <a href="https://pypi.org/project/requests/" target="_blank">requests</a>
# MAGIC - <a href="https://docs.python.org/3/library/csv.html" target="_blank">csv</a>
# MAGIC
# MAGIC ##### Spark Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession" target="_blank">SparkSession</a>

# COMMAND ----------

storageAccountKey=''
spark.conf.set("fs.azure.account.key.adlsadbdatalakehousedev.dfs.core.windows.net",storageAccountKey)
