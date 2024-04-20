-- Databricks notebook source
use catalog jdata_dev;

select current_catalog()

-- COMMAND ----------

create schema if not exists gold;

create or replace table gold.FactSongs
(
    artist_id STRING,
    artist_name STRING,
    duration DOUBLE,
    release STRING,
    tempo DOUBLE,
    time_signature DOUBLE,
    title STRING,
    year DOUBLE,
    processed_time TIMESTAMP
);

insert into gold.FactSongs
SELECT
  artist_id,
  artist_name,
  duration,
  release,
  tempo,
  time_signature,
  title,
  year,
  current_timestamp()
FROM landing.raw_dataset_songs;



-- COMMAND ----------

select * from landing.raw_dataset_songs
