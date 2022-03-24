-- Databricks notebook source
-- MAGIC %sql
-- MAGIC use f1_transform

-- COMMAND ----------

create table f1_transform.find_updated_dominant_team
using parquet
As
select team_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as average_points
from calculated_race_result
where race_year between 2011 and 2020 
group by team_name
having count(1) >=50
order by average_points desc


-- COMMAND ----------


