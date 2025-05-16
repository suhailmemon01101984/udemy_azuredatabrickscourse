# Databricks notebook source
# DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
# MAGIC %md
# MAGIC ##### Goal of this Notebook
# MAGIC  In this lesson, we are reading a set of json files from storage account using a manually specified schema/ddl and then applying different kinds of SQL typer operations like filter, selecting specific columns, where clause, sorting, distinct, getting first row, getting 10 rows using take, storing data in an array using collect, etc
# MAGIC
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC  CSV Source File Path : "abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv"
# MAGIC
# MAGIC
# MAGIC
# MAGIC JSON  File Path : "abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json"
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
# MAGIC | Spark Method | Purpose |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`show`** , **`display`** | Displays the top n rows of DataFrame in a tabular form |
# MAGIC | **`count`** | Returns the number of rows in the DataFrame |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`first`** , **`head`** | Returns the the first row |
# MAGIC | **`collect`** | Returns an array that contains all rows in this DataFrame |
# MAGIC | **`limit`** , **`take`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |
# MAGIC
# MAGIC - Other <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> methods: **`printSchema`**, **`display`**, **`createOrReplaceTempView`**
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html" target="_blank">GenericDataFrameWriter</a>: **`json`**, **`csv`**, **`mode (overwrite,append)`** 

# COMMAND ----------

storageAccountKey=''
spark.conf.set("fs.azure.account.key.dbrkcourse2025storagedev.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

sourceCSVFilePath='abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv'
sourceJSONFilePath='abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/json'

# COMMAND ----------

sourceJSONFileDF=spark.read.json(sourceJSONFilePath)

# COMMAND ----------

#updating row_id to integer, arrival in tonnes as decimal
sourceJSONFileSchemaDDL="DATE_OF_PRICING string,ROW_ID integer,STATE_NAME string,MARKET_NAME string,PRODUCTGROUP_NAME string,PRODUCT_NAME string,VARIETY string,ORIGIN string,ARRIVAL_IN_TONNES decimal(18,2),MINIMUM_PRICE string,MAXIMUM_PRICE string,MODAL_PRICE string"

# COMMAND ----------

sourceJSONFileDF=spark.read.schema(sourceJSONFileSchemaDDL).json(sourceJSONFilePath)


# COMMAND ----------

sourceJSONFileDF.select("PRODUCT_NAME")

# COMMAND ----------

sourceJSONFileDF.drop("ROW_ID")

# COMMAND ----------

sourceJSONFileDF.select("PRODUCT_NAME","ARRIVAL_IN_TONNES").filter("ARRIVAL_IN_TONNES > 100").show()


# COMMAND ----------

from pyspark.sql.functions import *

sourceJSONFileDF.select("PRODUCT_NAME","ARRIVAL_IN_TONNES").filter("ARRIVAL_IN_TONNES > 100").where(col("STATE_NAME") == "Andhra Pradesh").show()  


# COMMAND ----------

sourceJSONFileDF.select("PRODUCT_NAME","ARRIVAL_IN_TONNES").filter("ARRIVAL_IN_TONNES > 100").where(col("STATE_NAME") == "Andhra Pradesh").sort(desc("ARRIVAL_IN_TONNES")).show()


# COMMAND ----------

sourceJSONFileDF.select("PRODUCT_NAME","ARRIVAL_IN_TONNES").filter("ARRIVAL_IN_TONNES > 100").where(col("STATE_NAME") == "Andhra Pradesh").sort(("ARRIVAL_IN_TONNES")).show()

# COMMAND ----------

display(sourceJSONFileDF.select("STATE_NAME").distinct())

# COMMAND ----------

display(sourceJSONFileDF.count())

# COMMAND ----------

sourceJSONFileDF.first()

# COMMAND ----------

sourceJSONFileDF.head()

# COMMAND ----------

display(sourceJSONFileDF.take(10))

# COMMAND ----------

#stores data in an array
sourceJSONFileDF.collect()
