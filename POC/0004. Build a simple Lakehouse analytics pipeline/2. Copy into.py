# Databricks notebook source
username = spark.sql("select regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
database = f"copyinto_{username}_db"
# source = f"dbfs:/user/{username}/copy-into-demo"
source = "abfss://databrickspoc@datalakeliuyu.dfs.core.windows.net/exceed/ax7/managed_volume/copy-into-demo"

spark.sql(f"set c.username = '{username}'")
spark.sql(f"set c.database = {database}")
spark.sql(f"set c.source = '{source}'")

spark.sql("drop database if exists ${c.database} cascade")
spark.sql("create database ${c.database}")
spark.sql("use ${c.database}")

dbutils.fs.rm(source, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists user_ping_raw;
# MAGIC create table user_ping_raw (user_id string, ping integer, time timestamp) using json location ${c.source};
# MAGIC drop table if exists user_ids;
# MAGIC create table user_ids(user_id string);
# MAGIC INSERT INTO
# MAGIC   user_ids
# MAGIC VALUES
# MAGIC   ("potato_luver"),
# MAGIC   ("beanbag_lyfe"),
# MAGIC   ("default_username"),
# MAGIC   ("the_king"),
# MAGIC   ("n00b"),
# MAGIC   ("frodo"),
# MAGIC   ("data_the_kid"),
# MAGIC   ("el_matador"),
# MAGIC   ("the_wiz");
# MAGIC
# MAGIC drop function if exists get_ping;
# MAGIC CREATE FUNCTION get_ping() RETURNS INT RETURN int(rand() * 250);
# MAGIC
# MAGIC drop function if exists is_active;
# MAGIC CREATE FUNCTION is_active() RETURNS BOOLEAN RETURN CASE
# MAGIC     WHEN rand() >.25 THEN true
# MAGIC     ELSE false
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW FUNCTIONS is_active1

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO user_ping_raw
# MAGIC SELECT *,
# MAGIC   get_ping() ping,
# MAGIC   current_timestamp() time
# MAGIC FROM user_ids
# MAGIC WHERE CASE
# MAGIC     WHEN rand() >.25 THEN true
# MAGIC     ELSE false
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_ping_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists user_ping_target;
# MAGIC copy into user_ping_target
# MAGIC FROM
# MAGIC   ${c.source} fileformat = JSON FORMAT_OPTIONS("mergeSchema" = "true") copy_options("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_ping_target

# COMMAND ----------

# Drop database and tables and remove data

spark.sql("DROP DATABASE IF EXISTS ${c.database} CASCADE")
dbutils.fs.rm(source, True)
