# Databricks notebook source
# %fs ls "dbfs:/databricks-datasets/songs/"
# %fs ls "dbfs:/databricks-datasets/songs/data-001"
dbutils.fs.ls("dbfs:/")

# COMMAND ----------

# %fs head --maxBytes=10000 "dbfs:/databricks-datasets/songs/README.md"
# %fs head --maxBytes=10000 "dbfs:/databricks-datasets/songs/data-001/header.txt"
# %fs head --maxBytes=10000 "dbfs:/databricks-datasets/songs/data-001/part-00000"

dbutils.fs.head("dbfs:/databricks-datasets/songs/README.md", max_bytes=10000)
dbutils.fs.head("dbfs:/databricks-datasets/songs/data-001/header.txt", max_bytes=10000)
# dbutils.fs.head("dbfs:/databricks-datasets/songs/data-001/part-00000", max_bytes=10000)


# COMMAND ----------

from pyspark.sql.types import StructType, _parse_datatype_string
import json

# Read the header data
header = (
    spark.read.text(
        "dbfs:/databricks-datasets/songs/data-001/header.txt", wholetext=True
    )
    .first()
    .value
)
schema = _parse_datatype_string(header.replace("\n", ",").replace(":", " "))
print(schema)

# Now you can use the schema to read your CSV data
df = (
    spark.read.format("csv")
    .schema(schema)
    .option("sep", "\t")
    .load("dbfs:/databricks-datasets/songs/data-001/part*")
)
display(df)
print(df.count())
