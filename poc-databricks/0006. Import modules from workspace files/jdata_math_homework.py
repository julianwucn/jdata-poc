# Databricks notebook source
# MAGIC %run ./jdata_math
# MAGIC

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("n", "1", "input n: ")
dbutils.widgets.text("m", "1", "input m: ")

# COMMAND ----------

n = (dbutils.widgets.get("n"))
m = (dbutils.widgets.get("m"))
if n.isnumeric() and m.isnumeric():
    n_to_mth(n,m)
else:
    print("Please enter numbers")

# COMMAND ----------

spark.catalog.setCurrentCatalog("jdata_dev")
spark.catalog.setCurrentDatabase("landing")

df = spark.table("department").limit(1)
display(df)

# COMMAND ----------

# MAGIC %fs ls "dbfs:/FileStore/tables"

# COMMAND ----------

import urllib
urllib.request.urlretrieve("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet", "/Volumes/jdata_dev/landing/external_volume/yello_taxi_trips")

# COMMAND ----------

df = spark.read.parquet("/Volumes/jdata_dev/landing/external_volume/yello_taxi_trips")

display(df)
