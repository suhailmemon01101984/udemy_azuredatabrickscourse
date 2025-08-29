-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Data File Path in DataLake Storage Account
-- MAGIC
-- MAGIC CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
-- MAGIC
-- MAGIC JSON Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
-- MAGIC
-- MAGIC PARQUET Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/parquet"
-- MAGIC
-- MAGIC
-- MAGIC ###### Spark Session Methods
-- MAGIC - <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">SparkSessionMethods</a>  **`read`**,**`write`**, **`createDataFrame`** , **`sql`** ,  **`table`**   
-- MAGIC
-- MAGIC ###### Dataframes To/From SQL Conversions
-- MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html" target="_blank">DataFrame-SQLConversions</a> :**`createOrReplaceTempView`** ,**`spark.sql`**  ,**`createOrReplaceGlobalTempView`**

-- COMMAND ----------

storageAccountKey=''
spark.conf.set("fs.azure.account.key.dbrkcrse20251storagedev.dfs.core.windows.net",storageAccountKey)

-- COMMAND ----------

sourceCSVFilePath='abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv'
sourceJSONFilePath='abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json'
sourceParquetFilePath='abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/parquet'

-- COMMAND ----------

select * from global_temp.daily_pricing_global_temp_vw

-- COMMAND ----------

create table daily_pricing_csv_managed
as
select * from global_temp.daily_pricing_global_temp_vw


-- COMMAND ----------

select count(*) from daily_pricing_csv_managed

-- COMMAND ----------

insert into daily_pricing_csv_managed
select * from global_temp.daily_pricing_global_temp_vw


-- COMMAND ----------

alter table daily_pricing_csv_managed
add column datalake_updated_date date

-- COMMAND ----------

desc daily_pricing_csv_managed

-- COMMAND ----------

desc extended daily_pricing_csv_managed

-- COMMAND ----------

update daily_pricing_csv_managed
set datalake_updated_date = current_timestamp()

-- COMMAND ----------

select * from daily_pricing_csv_managed

-- COMMAND ----------

drop table daily_pricing_csv_managed

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sourceCSVFileDF = spark.sql("select * from global_temp.daily_pricing_global_temp_vw")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sourceCSVFileDF.write.saveAsTable("daily_pricing_csv_managed")

-- COMMAND ----------

desc extended daily_pricing_csv_managed

-- COMMAND ----------

drop table daily_pricing_csv_managed
