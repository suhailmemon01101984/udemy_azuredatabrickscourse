# Databricks notebook source
# DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
# MAGIC %md
# MAGIC
# MAGIC ##### Goal of this Notebook
# MAGIC  In this lesson, we are reading a set of parquet files from storage account using load method and then adding a column with value as currenttimestamp. then we perform different datetime operations on this column and create new columns. for eg: get the year, get the month, get the day of month, concatenating these 3 to form a date, using date_format to format a timestamp value to a date with a specific format, converting a column with string datatype to a date datatype and using date_format to display the resulting date into a specific format
# MAGIC
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC CSV Source File Path : "abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv"
# MAGIC
# MAGIC JSON File Path : "abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json"
# MAGIC
# MAGIC PARQUET Target  File Path : "abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/parquet"
# MAGIC
# MAGIC
# MAGIC ##### Spark Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession" target="_blank">SparkSession</a>
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html" target="_blank">GenericDataFrameReader</a>: **`json`**,**`csv`**,  **`option (header,inferSchema)`** ,  **`schema`**
# MAGIC
# MAGIC
# MAGIC ##### DateTime Methods
# MAGIC
# MAGIC **`current_timestamp()`** records the timestamp when the code is executed
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">ColumnFunctions</a>: **`cast`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions" target="_blank">Built-In DateTime Functions</a>: **`date_format`**, **`to_date`**,**`year`**, **`month`**, **`dayofmonth`**, **`minute`**, **`second`**
# MAGIC
# MAGIC ##### Date Time Functions
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`current_timestamp`** | Returns the current timestamp at the start of query evaluation as a timestamp column |
# MAGIC | **`date_format`** | Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument. |
# MAGIC | **`dayofmonth`** | Extracts the day of the month as an integer from a given date/timestamp/string |
# MAGIC
# MAGIC
# MAGIC #####  Date Time Formats
# MAGIC | Format | Meaning         | DataType | Sample Output              |
# MAGIC | ------ | --------------- | ------------ | ---------------------- |
# MAGIC | y      | year            | year         | 2020; 20               |
# MAGIC | D      | day-of-year     | number(3)    | 189                    |
# MAGIC | M/L    | month-of-year   | month        | 7; 07; Jul; July       |
# MAGIC | d      | day-of-month    | number(3)    | 28                     |
# MAGIC | Q/q    | quarter-of-year | number/text  | 3; 03; Q3; 3rd quarter |
# MAGIC | E      | day-of-week     | text         | Tue; Tuesday           |
# MAGIC

# COMMAND ----------

storageAccountKey=''
spark.conf.set("fs.azure.account.key.dbrkcourse2025storagedev.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

sourceCSVFilePath='abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv'
sourcePARQUETFilePath='abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/parquet'

# COMMAND ----------

sourcePARQUETFileDF=spark.read.format("parquet").load(sourcePARQUETFilePath)

# COMMAND ----------

from pyspark.sql import functions as func

sourcePARQUETTransFileDF=sourcePARQUETFileDF.withColumn("datalake_file_load_date",func.current_timestamp())


# COMMAND ----------

display(sourcePARQUETTransFileDF)

# COMMAND ----------

sourcePARQUETTransFileDF.select("datalake_file_load_date","DATE_OF_PRICING").\
withColumn("datalake_file_load_date_year",func.year("datalake_file_load_date")).\
withColumn("datalake_file_load_date_month",func.month("datalake_file_load_date")).\
withColumn("datalake_file_load_date_dayofmonth",func.dayofmonth("datalake_file_load_date")).\
withColumn("datalake_file_load_date_format",func.concat(func.year("datalake_file_load_date"),func.month("datalake_file_load_date"),func.dayofmonth("datalake_file_load_date"))).\
withColumn("datalake_file_load_date_format2",func.date_format("datalake_file_load_date","yyyy-MM-dd")).\
withColumn("PRICING_DATE",func.to_date("DATE_OF_PRICING","dd/MM/yyyy")).\
withColumn("PRICING_DATE_FORMAT",func.date_format("PRICING_DATE","yyyyMMdd")).display()  

# COMMAND ----------


