# Databricks notebook source
# MAGIC %md
# MAGIC ##### Goal of this Notebook
# MAGIC  In this lesson, we are reading a CSV file from a webURL, putting that data in a pandas dataframe, convert that dataframe to a spark dataframe and finally writing that spark dataframe to storage account
# MAGIC
# MAGIC
# MAGIC ##### Source File Details
# MAGIC Source File URL : "https://retailpricing.blob.core.windows.net/labs/lab1/PW_MW_DR_01012023.csv"
# MAGIC
# MAGIC Source File Ingestion Path Standard : "abfss://containername@storageaccountname.dfs.core.windows.net/foldername/subfoldername/subfoldername"
# MAGIC
# MAGIC Source File Ingestion Path : "abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv"
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
spark.conf.set("fs.azure.account.key.dbrkcourse2025storagedev.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

import pandas

# COMMAND ----------

sourceFileURL='https://retailpricing.blob.core.windows.net/labs/lab1/PW_MW_DR_01012023.csv'
bronzeLayerCSVFilePath='abfss://working-labs-dev@dbrkcourse2025storagedev.dfs.core.windows.net/bronze-dev/daily-pricing/csv'

# COMMAND ----------

pandas.read_csv(sourceFileURL)

# COMMAND ----------

sourceFilePandasDF=pandas.read_csv(sourceFileURL)

# COMMAND ----------

spark.createDataFrame(sourceFilePandasDF)

# COMMAND ----------

sourceFileSparkDF=spark.createDataFrame(sourceFilePandasDF)

# COMMAND ----------

print(sourceFilePandasDF)
display(sourceFileSparkDF)

# COMMAND ----------

sourceFileSparkDF.write.mode('overwrite').csv(bronzeLayerCSVFilePath)
