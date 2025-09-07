-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Source File Details
-- MAGIC CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze-dev/daily-pricing/csv"
-- MAGIC
-- MAGIC JSON Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze-dev/daily-pricing/json"
-- MAGIC
-- MAGIC PARQUET Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze-dev/daily-pricing/parquet"
-- MAGIC
-- MAGIC
-- MAGIC ###### Spark Session Methods
-- MAGIC - <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">SparkSessionMethods</a> :**`read`**,**`write`**,  **`sql`** ,  **`table`** ,  **`createDataFrame`**
-- MAGIC
-- MAGIC ###### SQL On Files
-- MAGIC - <a href="https://spark.apache.org/docs/2.2.1/sql-programming-guide.html#run-sql-on-files-directly" target="_blank">DirectSQLOnFiles</a> :**`select`** ,**`view`**  ,**`temp view`** ,**`Common Table Expressions*CTE)`** , **`external Tables`**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storageAccountKey=''
-- MAGIC spark.conf.set("fs.azure.account.key.dbrkcrse20251storagedev.dfs.core.windows.net",storageAccountKey)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sourceCSVFilePath='abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv'
-- MAGIC sourceJSONFilePath='abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json'
-- MAGIC sourceParquetFilePath='abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/parquet'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(sourceJSONFilePath)

-- COMMAND ----------

select * from json.`abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json/part-00000-tid-5849245006361592063-aad08ddc-0c45-40a6-9257-502a4585c0a8-24-1-c000.json`

-- COMMAND ----------

select * from json.`abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json/`

-- COMMAND ----------

create or replace view daily_pricing_json_external_vw
as
select * from json.`abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json/`

-- COMMAND ----------

select count(*) from daily_pricing_json_external_vw

-- COMMAND ----------

describe extended daily_pricing_json_external_vw

-- COMMAND ----------

create or replace table daily_pricing_json_external_table
as
select * from json.`abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json/`

-- COMMAND ----------

desc extended daily_pricing_json_external_table

-- COMMAND ----------

create table daily_pricing_csv_external_table
(
DATE_OF_PRICING	string,
ROW_ID	string,
STATE_NAME	string,
MARKET_NAME	string,
PRODUCTGROUP_NAME	string,
PRODUCT_NAME	string,
VARIETY	string,
ORIGIN	string,
ARRIVAL_IN_TONNES	string,
MINIMUM_PRICE	string,
MAXIMUM_PRICE	string,
MODAL_PRICE	string
)
using csv
options (header="true",delimiter=",")
location "${sourceCSVFilePath}"
