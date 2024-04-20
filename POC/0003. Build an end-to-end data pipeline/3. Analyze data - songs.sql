-- Databricks notebook source
select * from dbo.FactSongs;

delete 
from dbo.FactSongs t
where t.artist_name is null or t.year = 0;

select count(*)
from dbo.FactSongs t
where t.artist_name is null or t.year = 0;

-- COMMAND ----------

select
  s.artist_name,
  s.year,
  count(*) as total_songs
from
  dbo.FactSongs s
group by
  s.artist_name,
  s.year
order by
  s.year desc,
  total_songs desc,
  s.artist_name
