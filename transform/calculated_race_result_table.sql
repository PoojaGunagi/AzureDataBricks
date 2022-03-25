-- Databricks notebook source
create database if not exists

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

select * from result_tb

-- COMMAND ----------

select * from driver_tb

-- COMMAND ----------

select * from constructor_tb

-- COMMAND ----------

create table f1_transform.calculated_race_result
using parquet
as 
select  race_tb.race_year,constructor_tb.team as team_name, driver_tb.name as driver_name, result_tb.position, result_tb.points,11 - result_tb.position as calculated_points
from result_tb
join driver_tb on (result_tb.driver_Id=driver_tb.driver_id)
join constructor_tb on (constructor_tb.constructor_id=result_tb.constructor_id)
join race_tb on (race_tb.race_id=result_tb.race_id)


-- COMMAND ----------

  
