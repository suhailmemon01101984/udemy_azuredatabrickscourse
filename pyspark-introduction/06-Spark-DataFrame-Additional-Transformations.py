# Databricks notebook source
# DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
# MAGIC %md
# MAGIC ##### Goal of this Notebook
# MAGIC  In this lesson, we are reading a set of csv files from storage account using load method and then adding a column with some transformations i.e. converting tonnes to kgs, doing a group by operation plus aggregate functions to see total of that column in kgs by some other columns and finally writing back the data to storage account in parquet format. finally we use the same load method to read back the parquet data and displaying it
# MAGIC
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC  CSV Source File Path : "abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv"
# MAGIC
# MAGIC PARQUET Target  File Path : "abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/parquet"
# MAGIC  
# MAGIC ##### Spark Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession" target="_blank">SparkSession</a>
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html" target="_blank">GenericDataFrameReader</a>: **`json`**,**`csv`**,  **`option (header,inferSchema)`** ,  **`schema`**
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">PySparkSQLFunctions</a>: **`col`** , **`alias`** , **`cast`**
# MAGIC
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> transformations and actions: 
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`limit`** , **`take`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |
# MAGIC
# MAGIC - Other <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> methods: **`printSchema`**, **`display`**, **`createOrReplaceTempView`**
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html" target="_blank">GenericDataFrameWriter</a>: **`parquet`**, **`csv`**, **`mode (overwrite,append)`** 

# COMMAND ----------

storageAccountKey=''
spark.conf.set("fs.azure.account.key.dbrkcourse2025storagedev.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

sourceCSVFilePath='abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv'
targetPARQUETFilePath='abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/parquet'

# COMMAND ----------

sourceCSVFileDF=spark.read.load(sourceCSVFilePath,format="csv")

# COMMAND ----------

sourceCSVFileDF=spark.read.load(sourceCSVFilePath,format="csv",header="true")

# COMMAND ----------

#inferschema=true is an intensive operation. should be used during development but in production scenarios where performance is key avoid using it. inferscheam=true will scan through the data in the file and determine the data type of each column. you should do this sparingly instead of every single time. much more preferred to define the schema manually with the DDL method or the struct type/struct field method that's shown in previous lessons
sourceCSVFileDF=spark.read.load(sourceCSVFilePath,format="csv",header="true",inferSchema="true")

# COMMAND ----------

from pyspark.sql import functions as func

sourceCSVFileTransDF=sourceCSVFileDF.withColumn("ARRIVAL_IN_KILOGRAMS",func.col("ARRIVAL_IN_TONNES")*1000)


# COMMAND ----------

display(sourceCSVFileTransDF)

# COMMAND ----------

from pyspark.sql import functions as func

sourceCSVFileTransDF.groupBy("STATE_NAME","PRODUCT_NAME").agg(func.sum("ARRIVAL_IN_KILOGRAMS").alias("TOTAL_ARRIVAL_IN_KILOGRAMS")).orderBy(func.col("TOTAL_ARRIVAL_IN_KILOGRAMS").desc()).display()

# COMMAND ----------

#generic data frame writer and reader uses the parquet file as the standard format while reading and writing
sourceCSVFileTransDF.write.mode("overwrite").parquet(targetPARQUETFilePath)

# COMMAND ----------

spark.read.load(targetPARQUETFilePath,format="parquet")

# COMMAND ----------

spark.read.load(targetPARQUETFilePath,format="parquet").display()
