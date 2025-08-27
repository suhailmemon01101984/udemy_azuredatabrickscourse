# Databricks notebook source
# MAGIC %md
# MAGIC ##### Data File Path in DataLake Storage Account
# MAGIC
# MAGIC CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC JSON Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
# MAGIC
# MAGIC PARQUET Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/parquet"
# MAGIC
# MAGIC
# MAGIC ###### Spark Session Methods
# MAGIC - <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">SparkSessionMethods</a>  **`read`**,**`write`**, **`createDataFrame`** , **`sql`** ,  **`table`**   
# MAGIC
# MAGIC ###### Dataframes To/From SQL Conversions
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html" target="_blank">DataFrame-SQLConversions</a> :**`createOrReplaceTempView`** ,**`spark.sql`**  ,**`createOrReplaceGlobalTempView`**
