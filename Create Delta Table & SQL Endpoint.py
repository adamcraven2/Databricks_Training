# Databricks notebook source
# List secrets created with API
dbutils.secrets.list('new-scope')

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="new-scope", key="sa-key")

# Access data lake by account key
spark.conf.set(
    "fs.azure.account.key.dlengineerpractice.dfs.core.windows.net",
    storage_account_key)

# COMMAND ----------

# List files in container
dbutils.fs.ls("abfss://data@dlengineerpractice.dfs.core.windows.net/")

# COMMAND ----------

# Set the data lake file location
file_location = "abfss://data@dlengineerpractice.dfs.core.windows.net/"

# Read in the data to dataframe df
df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimeter", ",").load(file_location)

# Create temp table from dataframe
df.createOrReplaceTempView("myData")

# COMMAND ----------

# Save CSV as delta table in airplanes folder
df.write.format("delta").save("abfss://delta-tables@dlengineerpractice.dfs.core.windows.net/airplanes")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Catalog container
# MAGIC CREATE DATABASE DataEngineerTest;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Create tables as select to store data from temp table
# MAGIC CREATE TABLE DataEngineerTest.Airplanes 
# MAGIC AS 
# MAGIC SELECT *
# MAGIC -- Select from temp table which was created from initial dataframe
# MAGIC FROM myData

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select data from table created above
# MAGIC SELECT *
# MAGIC FROM dataengineertest.airplanes
