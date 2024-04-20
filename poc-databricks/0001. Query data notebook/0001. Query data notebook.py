# Databricks notebook source
#display(dbutils.fs.ls('/databricks-datasets'))
display(dbutils.fs.ls('/'))

# COMMAND ----------

f = open('/databricks-datasets/README.md', 'r')
print(f.read())

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog(), current_schema(), current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog jdata_dev;
# MAGIC
# MAGIC CREATE TABLE if not exists landing.department
# MAGIC (
# MAGIC   deptcode  INT,
# MAGIC   deptname  STRING,
# MAGIC   location  STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO landing.department VALUES
# MAGIC   (10, 'FINANCE', 'EDINBURGH'),
# MAGIC   (20, 'SOFTWARE', 'PADDINGTON'),
# MAGIC   (30, 'SALES', 'MAIDSTONE'),
# MAGIC   (40, 'MARKETING', 'DARLINGTON'),
# MAGIC   (50, 'ADMIN', 'BIRMINGHAM');

# COMMAND ----------


diamonds = (spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
)
display(diamonds)
diamonds.write.format("csv").mode("overwrite").save("/Volumes/jdata_dev/landing/external_volume/diamonds.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS diamonds;
# MAGIC -- CREATE TABLE diamonds USING DELTA LOCATION '/mnt/delta/diamonds/'
# MAGIC
# MAGIC SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_'), current_user()
