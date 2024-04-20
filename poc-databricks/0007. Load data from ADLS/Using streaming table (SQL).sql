-- Databricks notebook source
-- # exploring
list '/Volumes/jdata_dev/landing/external_volume';

list 'abfss://jdatadev@jdatadatalake.dfs.core.windows.net/landing_externalvolume';

-- COMMAND ----------

select * from read_files('/Volumes/jdata_dev/landing/external_volume/ny-yellow-taxi-location-info.csv', format=>'csv') limit 10

-- COMMAND ----------

use catalog jdata_dev;
use schema landing;
select current_catalog(), current_schema();

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE streaming_table_taxidata as
select
  *
from
  stream read_files(
    '/Volumes/jdata_dev/landing/external_volume/ny-yellow-taxi-location-info.csv',
    format => 'csv'
  )
