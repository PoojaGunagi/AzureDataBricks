-- Databricks notebook source
-- MAGIC %python
-- MAGIC html ="""<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula-1 Team </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------



select
team_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as average_points,
RANK() over(order by avg(calculated_points) desc) team_rank
from f1_transform.calculated_race_result
where race_year between 2011 and 2020 
group by team_name
having count(1) >=50
order by average_points desc

-- COMMAND ----------

create or replace temp view v_viz_dominantteams
as
select
team_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as average_points,
RANK() over(order by avg(calculated_points) desc) team_rank
from f1_transform.calculated_race_result
where race_year between 2011 and 2020 
group by team_name
having count(1) >= 100
order by average_points desc


-- COMMAND ----------

select
race_year,
team_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as average_points
from f1_transform.calculated_race_result
where team_name in ( select team_name from v_viz_dominantteams where team_rank <= 10)
group by race_year,team_name
order by average_points desc

-- COMMAND ----------

select
race_year,
team_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as average_points
from f1_transform.calculated_race_result
where team_name in ( select team_name from v_viz_dominantteams where team_rank <= 10)
group by race_year,team_name
order by average_points desc

-- COMMAND ----------

select
race_year,
team_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as average_points
from f1_transform.calculated_race_result
where team_name in ( select team_name from v_viz_dominantteams where team_rank <= 10)
group by race_year,team_name
order by average_points desc

-- COMMAND ----------


