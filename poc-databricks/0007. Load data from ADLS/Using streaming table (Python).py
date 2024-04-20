# Databricks notebook source
# exploring
source = "/Volumes/jdata_dev/landing/external_volume"
file = source + "/ny-yellow-taxi-location-info.csv"
# source = 'abfss://jdatadev@jdatadatalake.dfs.core.windows.net/landing_externalvolume'
dbutils.fs.ls(source)

# COMMAND ----------

display(spark.read.format('csv').load(file).limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- use catalog jdata_dev;
# MAGIC select current_catalog();

# COMMAND ----------

import dlt

@dlt.table
def streaming_table_taxidata_python():
    return (
        spark.readStream.format('cloudFiles')
        .option("cloudFiles.format", "csv")
        .load(f"{file}")
    )
