# Databricks notebook source
# MAGIC %sql
# MAGIC use f1_processed;

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.widgets.text("p_data_source","")

# COMMAND ----------

# MAGIC %python
# MAGIC   dbutils.widgets.text("p_file_date","2021-03-21")
# MAGIC v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS f1_delta_transform.calculated_race_results
          (
              race_year int,
              team_name STRING,
              driver_id int,
              driver_name String,
              race_id int,
              position int,
              points int,
              calculated_points int,
              created_date  timestamp,
              updated_date timestamp
              
          )
          using delta
          
          """)

# COMMAND ----------



# COMMAND ----------

spark.sql(f"""
            create or replace temp view race_result_updated_vw
            as
            select  race_tb.race_year,driver_tb.driver_id,race_tb.race_id,constructor_tb.team as team_name, driver_tb.name as driver_name, result_tb.position, result_tb.points,11 - result_tb.position as calculated_points
            from result_tb
            join driver_tb on (result_tb.driver_Id=driver_tb.driver_id)
            join constructor_tb on (constructor_tb.constructor_id=result_tb.constructor_id)
            join race_tb on (race_tb.race_id=result_tb.race_id)
            where result_tb.position<=10
            and result_tb.file_date='{v_file_date}'

""")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_transform.calculated_race_result
# MAGIC using parquet
# MAGIC as 
# MAGIC select  race_tb.race_year,constructor_tb.team as team_name, driver_tb.name as driver_name, result_tb.position, result_tb.points,11 - result_tb.position as calculated_points
# MAGIC from result_tb
# MAGIC join driver_tb on (result_tb.driver_Id=driver_tb.driver_id)
# MAGIC join constructor_tb on (constructor_tb.constructor_id=result_tb.constructor_id)
# MAGIC join race_tb on (race_tb.race_id=result_tb.race_id)

# COMMAND ----------


