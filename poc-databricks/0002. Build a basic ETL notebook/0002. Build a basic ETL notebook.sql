-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls("/tmp/liuyu_mu_minxuewuoutlook_onmicrosoft_com/_schema/etl_quickstart", )
-- MAGIC # dbutils.fs.ls("dbfs:/databricks-datasets/structured-streaming/events/")
-- MAGIC
-- MAGIC # dbutils.fs.help()

-- COMMAND ----------


use catalog jdata_dev;

select current_catalog();


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, current_timestamp
-- MAGIC
-- MAGIC #variables
-- MAGIC file_path = "dbfs:/databricks-datasets/structured-streaming/events/"
-- MAGIC user_name = spark.sql("select regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
-- MAGIC table_name = f"landing.raw_dataset_events"
-- MAGIC checkpoint_path = f"/tmp/{user_name}/_checkpoint/etl_quickstart"
-- MAGIC schema_location = f"/tmp/{user_name}/_schema/etl_quickstart"  # Define schema location
-- MAGIC
-- MAGIC #clear
-- MAGIC spark.sql(f"drop table if exists {table_name}")
-- MAGIC dbutils.fs.rm(checkpoint_path, True)
-- MAGIC
-- MAGIC #auto loader
-- MAGIC (spark.readStream
-- MAGIC     .format("cloudFiles")
-- MAGIC     .option("cloudFiles.format", "json")
-- MAGIC     .option("cloudFiles.schemaLocation", schema_location)
-- MAGIC     .load(file_path)
-- MAGIC     .select(
-- MAGIC         "*",
-- MAGIC         col("_metadata.file_path").alias("source_file"),  # Use col function correctly to select and alias the column
-- MAGIC         current_timestamp().alias("processing_time")  # Ensure you call the current_timestamp function
-- MAGIC     )
-- MAGIC     .writeStream
-- MAGIC     .option("checkpointLocation", checkpoint_path)
-- MAGIC     .trigger(availableNow=True)
-- MAGIC     .toTable(table_name)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Read the table
-- MAGIC df = spark.table(table_name)
-- MAGIC df = df.filter("col1=true").update()
-- MAGIC x=1
-- MAGIC x-2
-- MAGIC
-- MAGIC x="abc"
-- MAGIC x="xyz"
-- MAGIC
-- MAGIC x=[1, 2, 3]
-- MAGIC x[0] = 0
-- MAGIC
-- MAGIC # Display the table
-- MAGIC display(df)
