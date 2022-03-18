-- Databricks notebook source
use f1_transform

-- COMMAND ----------

select driver_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as average_points
from calculated_race_result
where race_year between 2011 and 2020 
group by driver_name
having count(1) >=50
order by average_points desc


-- COMMAND ----------


