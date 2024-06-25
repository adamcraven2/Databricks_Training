# Databricks notebook source
# Get and store secret in variable
storage_account_key = dbutils.secrets.get(scope="demo-scope", key="sa-key")

# COMMAND ----------

# Access data lake by account key
spark.conf.set(
    "fs.azure.account.key.dlengineerpractice.dfs.core.windows.net",
    storage_account_key)

# COMMAND ----------

# List files in container
dbutils.fs.ls("abfss://data@dlengineerpractice.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select data from table created above
# MAGIC SELECT *
# MAGIC FROM dataengineertest.airplane_passengers
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show metadata about the table
# MAGIC DESCRIBE DETAIL dataengineertest.airplane_passengers
# MAGIC
# MAGIC -- numFiles - How many files the data is split into
# MAGIC -- minReaderVersion - Version of the table that is active
# MAGIC

# COMMAND ----------

# Create dataframe from catalog table
airline_data = spark.table("dataengineertest.airplane_passengers")
# Only show 100 rows
display(airline_data.limit(100))

# COMMAND ----------

# Select columns and filter by passengers who are Satisfied, limit rows to 1000
airline_data.select("Gender", "Customer_Type", "Age", "Type_of_Travel", "Class", "Satisfaction", "Flight_Distance", "Departure_Delay", "Arrival_Delay")\
    .filter("Satisfaction = 'Satisfied'")\
    .limit(1000)\
    .display()

# COMMAND ----------

# Select average flight distance by customer type
airline_data.select("Customer_Type", "Flight_Distance").groupBy("Customer_Type").agg({'Flight_Distance': 'avg'}).display()

# COMMAND ----------

# Select columns and filter by passengers who are Satisfied and were in Economy Class, limit rows to 1000
satisfied_eco_passengers_data = airline_data.select("Gender", "Customer_Type", "Age", "Type_of_Travel", "Class", "Satisfaction", "Flight_Distance", "Departure_Delay", "Arrival_Delay")\
    .filter("Satisfaction = 'Satisfied' and Class = 'Economy'")\
    .limit(1000)\

# Shows the dataframe
satisfied_eco_passengers_data.display()


# COMMAND ----------

satisfied_eco_passengers_data.write.format('delta').saveAsTable('dataengineertest.satisfied_eco_passengers', path='abfss://delta-tables@dlengineerpractice.dfs.core.windows.net/satisfied_eco_passengers')
