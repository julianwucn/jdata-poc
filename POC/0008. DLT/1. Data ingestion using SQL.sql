-- Databricks notebook source
-- Bronze / ingestion tables
--currency
create or refresh streaming live table bronze.EACurrencies 
tableproperties ("layer" = "bronze") 
as
select
  *,
  input_file_name() as _FileName,
  current_timestamp() as _ProcessingTime
from
  cloud_files(
    "/Volumes/jdata_dev/landing/external_volume/csv/ax7EACurrencies.csv",
    "csv",
    map(
      "delimiter", ",",
      "header", "true",
      "inferSchema", "true",
      "cloudFiles.schemaLocation", "/mnt/temp/schemalocaiton/"
    )
  );

-- COMMAND ----------

--Customer
create or refresh streaming live table bronze.EACustolmers 
tableproperties ("layer" = "bronze") 
as
select
  *,
  input_file_name() as _FileName,
  current_timestamp() as _ProcessingTime
from
  cloud_files(
    "/Volumes/jdata_dev/landing/external_volume/csv/ax7EACustomers.csv",
    "csv",
    map(
      "delimiter", ",",
      "header", "true",
      "inferSchema", "true",
      "cloudFiles.schemaLocation", "/mnt/temp/schemalocaiton/"
    )
  );

-- COMMAND ----------

--Product
create or refresh streaming live table bronze.EAProducts 
tableproperties ("layer" = "bronze") 
as
select
  *,
  input_file_name() as _FileName,
  current_timestamp() as _ProcessingTime
from
  cloud_files(
    "/Volumes/jdata_dev/landing/external_volume/csv/ax7EAProducts.csv",
    "csv",
    map(
      "delimiter", ",",
      "header", "true",
      "inferSchema", "true",
      "cloudFiles.schemaLocation", "/mnt/temp/schemalocaiton/"
    )
  );

-- COMMAND ----------

--Product
create or refresh streaming live table bronze.EASalesOrders 
tableproperties ("layer" = "bronze") 
as
select
  *,
  input_file_name() as _FileName,
  current_timestamp() as _ProcessingTime
from
  cloud_files(
    "/Volumes/jdata_dev/landing/external_volume/csv/ax7EASalesOrders.csv",
    "csv",
    map(
      "delimiter", ",",
      "header", "true",
      "inferSchema", "true",
      "cloudFiles.schemaLocation", "/mnt/temp/schemalocaiton/"
    )
  );

-- COMMAND ----------

--Enum Values
create or refresh streaming live table bronze.EnumValues 
tableproperties ("layer" = "bronze") 
as
select
  *,
  input_file_name() as _FileName,
  current_timestamp() as _ProcessingTime
from
  cloud_files(
    "/Volumes/jdata_dev/landing/external_volume/csv/ax7EnumValues.csv",
    "csv",
    map(
      "delimiter", ",",
      "header", "true",
      "inferSchema", "true",
      "cloudFiles.schemaLocation", "/mnt/temp/schemalocaiton/"
    )
  );
