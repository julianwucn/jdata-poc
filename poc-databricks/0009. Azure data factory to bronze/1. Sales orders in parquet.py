# Databricks notebook source
bronze_path = "abfss://mdatadev01@mdatadatalake.dfs.core.windows.net/bronze/ax7_easalesorders"

dbutils.fs.ls(bronze_path)


# COMMAND ----------

df = spark.read.format("delta").load(bronze_path)

print(df.count())

display(df)

# COMMAND ----------

df.write.saveAsTable("mdata_dev.bronze.ax7_easalesorders")

# COMMAND ----------

spark.read.table("mdata_dev.bronze.ax7_easalesorders")
display(df.count())
