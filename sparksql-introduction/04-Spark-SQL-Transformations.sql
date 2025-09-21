-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Source File Details
-- MAGIC CSV Source File Path : "abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv"
-- MAGIC
-- MAGIC
-- MAGIC ###### Spark Session Methods
-- MAGIC - <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">SparkSessionMethods</a> :**`read`**,**`write`**,  **`sql`** ,  **`table`** ,  **`createDataFrame`**
-- MAGIC
-- MAGIC ###### SQL Transformations
-- MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html" target="_blank">SQLFunctions</a> :**`select`** ,**`distinct`**  ,**`where`** ,**`order by`**

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

SELECT * FROM daily_pricing_csv_managed

-- COMMAND ----------

SELECT PRODUCT_NAME , ARRIVAL_IN_TONNES FROM daily_pricing_csv_managed

-- COMMAND ----------

SELECT PRODUCT_NAME , ARRIVAL_IN_TONNES FROM daily_pricing_csv_managed
WHERE ARRIVAL_IN_TONNES > 100

-- COMMAND ----------

SELECT PRODUCT_NAME , ARRIVAL_IN_TONNES FROM daily_pricing_csv_managed
WHERE ARRIVAL_IN_TONNES > 100
AND STATE_NAME = "Andhra Pradesh"

-- COMMAND ----------

SELECT PRODUCT_NAME , ARRIVAL_IN_TONNES FROM daily_pricing_csv_managed
WHERE ARRIVAL_IN_TONNES > 100
AND STATE_NAME = "Andhra Pradesh"
ORDER BY ARRIVAL_IN_TONNES DESC

-- COMMAND ----------

SELECT DISTINCT STATE_NAME FROM daily_pricing_csv_managed LIMIT 10

-- COMMAND ----------

SELECT PRODUCT_NAME , ARRIVAL_IN_TONNES 
,ARRIVAL_IN_TONNES * 1000 AS ARRIVAL_IN_KILOGRAMS
 FROM daily_pricing_csv_managed

-- COMMAND ----------

SELECT STATE_NAME
,PRODUCT_NAME
,SUM(ARRIVAL_IN_TONNES) as TOTAL_ARRIVAL_IN_TONNES
FROM daily_pricing_csv_managed
GROUP BY 
STATE_NAME
,PRODUCT_NAME

-- COMMAND ----------

SELECT STATE_NAME
,PRODUCT_NAME
,SUM(ARRIVAL_IN_TONNES) as TOTAL_ARRIVAL_IN_TONNES
FROM daily_pricing_csv_managed
GROUP BY 
STATE_NAME
,PRODUCT_NAME
ORDER BY TOTAL_ARRIVAL_IN_TONNES DESC

-- COMMAND ----------

CREATE TABLE daily_pricing_aggregate_managed_table AS
SELECT STATE_NAME
,PRODUCT_NAME
,SUM(ARRIVAL_IN_TONNES) as TOTAL_ARRIVAL_IN_TONNES
FROM daily_pricing_csv_managed
GROUP BY 
STATE_NAME
,PRODUCT_NAME
