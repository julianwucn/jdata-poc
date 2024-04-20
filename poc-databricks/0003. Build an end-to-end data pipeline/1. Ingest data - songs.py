# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC use catalog jdata_dev;
# MAGIC select current_catalog();
# MAGIC

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/songs/data-001/")

# COMMAND ----------

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# variables
file_path = "dbfs:/databricks-datasets/songs/data-001/part*"

table_name = "landing.raw_dataset_songs"
checkpoint_path = "/tmp/pipeline_get_started/_checkpoint/song_data"
schema = StructType(
    [
        StructField("artist_id", StringType(), True),
        StructField("artist_lat", DoubleType(), True),
        StructField("artist_long", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("end_of_fade_in", DoubleType(), True),
        StructField("key", IntegerType(), True),
        StructField("key_confidence", DoubleType(), True),
        StructField("loudness", DoubleType(), True),
        StructField("release", StringType(), True),
        StructField("song_hotnes", DoubleType(), True),
        StructField("song_id", StringType(), True),
        StructField("start_of_fade_out", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("time_signature", DoubleType(), True),
        StructField("time_signature_confidence", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("partial_sequence", IntegerType(), True),
    ]
)

# (
#     spark.readStream.format("cloudFiles")
#     .schema(schema)
#     .option("cloudFiles.format", "csv")
#     .option("sep", "\t")
#     .load(file_path)
#     .writeStream.option("checkpointLocation", checkpoint_path)
#     .trigger(availableNow=True)
#     .toTable(table_name)
# )
df =(spark.read.format("csv")
     .options(header = False, sep = "\t")
     .schema(schema)
     .load(file_path)
)

print(df.count())
display(df.limit(10))

df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from landing.raw_dataset_songs limit(10)
