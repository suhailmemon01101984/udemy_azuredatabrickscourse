# Databricks notebook source
# MAGIC %md
# MAGIC ##### Data File Path in DataLake Storage Account
# MAGIC
# MAGIC CSV Source File Path : "abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv"
# MAGIC
# MAGIC JSON Source  File Path : "abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json"
# MAGIC
# MAGIC PARQUET Source  File Path : "abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/parquet"
# MAGIC
# MAGIC
# MAGIC ###### Spark Session Methods
# MAGIC - <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">SparkSessionMethods</a>  **`read`**,**`write`**, **`createDataFrame`** , **`sql`** ,  **`table`**   
# MAGIC
# MAGIC ###### Dataframes To/From SQL Conversions
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html" target="_blank">DataFrame-SQLConversions</a> :**`createOrReplaceTempView`** ,**`spark.sql`**  ,**`createOrReplaceGlobalTempView`**

# COMMAND ----------

storageAccountKey=''
spark.conf.set("fs.azure.account.key.dbrkcrse20251storagedev.dfs.core.windows.net",storageAccountKey)


# COMMAND ----------

sourceCSVFilePath='abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv'
sourceJSONFilePath='abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json'
sourceParquetFilePath='abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/parquet'

# COMMAND ----------

sourceCSVFileDF=spark.read.option("header","true").csv(sourceCSVFilePath)

# COMMAND ----------

sourceCSVFileDF.createOrReplaceTempView("daily_pricing_temp_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from daily_pricing_temp_vw

# COMMAND ----------

spark.sql("select * from daily_pricing_temp_vw")

# COMMAND ----------

spark.table("daily_pricing_temp_vw")

# COMMAND ----------

sourceCSVFileDF.createOrReplaceGlobalTempView("daily_pricing_global_temp_vw")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.daily_pricing_global_temp_vw
