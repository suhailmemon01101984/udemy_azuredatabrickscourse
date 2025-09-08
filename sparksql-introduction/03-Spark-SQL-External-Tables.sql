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

--before running below code first go to azure portal -> Access Connector for Azure Databricks --> hit create to create the connector --> give resource group as databrickscourse2025_1-resourcegroup-dev -> give name as bronzedevconnector and leave everything else as default and create the connector. next click on your connector and copy the value of resource id and store it in a textpad -> next go inside databricks --> catalog --> click on external data --> go under credentials --> create credential -> give credential name: bronze_dev_cred -> for the access connector id field, paste the resource id value -> then hit create -> then go under your azure storage account dbrkcrse20251storagedev -> access control -> role assignment -> add role assigment -> search for Storage Blob Data Owner -> hit next -> change option of assigned access to "managed identity" -> select members -> under managed identiy dropdown choose "access connector for azure databricks" --> choose bronzedevconnector -->click select -->click review and assign -> now this role is assigned to your access connector bronzedevconnector. now wait for 5 mins and run below sql:

CREATE EXTERNAL LOCATION bronze_dev_loc
URL 'abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev'
WITH (STORAGE CREDENTIAL `bronze_dev_cred`)
COMMENT 'External location for bronze dev data'


-- COMMAND ----------

grant read files on external location bronze_dev_loc to `databrickscourse2025_1@outlook.com`

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
location "abfss://working-labs-dev@dbrkcrse20251storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv"

-- COMMAND ----------

desc extended daily_pricing_csv_external_table

-- COMMAND ----------

select * from daily_pricing_csv_external_table

-- COMMAND ----------

drop view daily_pricing_json_external_vw;
drop table daily_pricing_json_external_table;
DROP TABLE IF EXISTS databricks_dev.default.daily_pricing_csv_external_table;
