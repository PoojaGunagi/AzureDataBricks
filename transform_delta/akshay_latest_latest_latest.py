# Databricks notebook source
# MAGIC %run "/includes/configuration"

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.widgets.text("p_data_source","")

# COMMAND ----------

# MAGIC %python
# MAGIC   dbutils.widgets.text("p_file_date","2021-04-18")
# MAGIC v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %sql use f1_delta_ingest;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------




# COMMAND ----------

races_df = spark.read.format("delta").load("/mnt/adls27/delta/race_tb")

# COMMAND ----------

display(races_df)

# COMMAND ----------

renamed_race_df=races_df.withColumnRenamed("date","race_date")

# COMMAND ----------

display(renamed_race_df)

# COMMAND ----------

circuits_df = spark.read.format("delta").load("/mnt/adls27/delta/circuit_tb")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_renamed_df=circuits_df.withColumnRenamed("location","circuit_location")

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

driver_df = spark.read.format("delta").load("dbfs:/mnt/adls27/delta/driver_tb")

# COMMAND ----------

display(driver_df)

# COMMAND ----------

driver_renamed_df=driver_df.withColumnRenamed("name","driver_name").withColumnRenamed("number","driver_number").withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

display(driver_renamed_df)

# COMMAND ----------

constructors_df = spark.read.format("delta").load("dbfs:/mnt/adls27/delta/constructor_tb")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

results_df = spark.read.format("delta").load("/mnt/adls27/delta/result_tb")

# COMMAND ----------

display(results_df)

# COMMAND ----------

result_renamed_df=results_df.withColumnRenamed("time","race_time")

# COMMAND ----------

display(races_df)

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

jo = circuits_renamed_df.join(renamed_race_df,circuits_renamed_df.circuit_id == renamed_race_df.circuit_id,"inner")

# COMMAND ----------


results_join_df = result_renamed_df.join(jo,jo.race_id==result_renamed_df.race_Id,"inner")\
.join(driver_renamed_df,driver_renamed_df.driver_id==result_renamed_df.driver_Id,"inner")\
.join(constructors_df,constructors_df.constructor_id==result_renamed_df.constructor_Id,"inner").drop(renamed_race_df.race_id).drop(renamed_race_df.time)

# COMMAND ----------

display(jo)

# COMMAND ----------

display(result_renamed_df)

# COMMAND ----------

display(driver_renamed_df)

# COMMAND ----------

display(renamed_race_df)

# COMMAND ----------

display(results_join_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

select_df = results_join_df.select(col('race_year'),col('race_name'),col('race_date'),col('circuit_location'),col('driver_name'),col('driver_number'),col('driver_nationality'),col('grid'),col('fastest_lap'),col('race_time'),col('points'),col("position"),col("team")).withColumn("current_date",current_timestamp())

# COMMAND ----------

display(select_df)

# COMMAND ----------

# MAGIC %sql create database if not exists f1_delta_transform
# MAGIC location "/mnt/adls27/transformnew/"

# COMMAND ----------

select_df.write.mode("Overwrite").format("delta").saveAsTable("f1_delta_transform.join_result_tb")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_delta_transform.join_result_tb

# COMMAND ----------

driver_standard_df=select_df.groupBy("race_year","driver_name","driver_nationality").agg(sum("points").alias("total_points"),
                                                                                         count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(driver_standard_df)

# COMMAND ----------

display(driver_standard_df.filter("race_year=2020"))

# COMMAND ----------

from pyspark.sql.functions import desc,rank
from pyspark.sql.window import *

# COMMAND ----------

somethingsomething = Window.partitionBy("race_year").orderBy(desc("total_points"))
    

# COMMAND ----------

r_df =  driver_standard_df.withColumn("rank",rank().over(somethingsomething))

# COMMAND ----------

display(r_df)

# COMMAND ----------

final_df=select_df.withColumn("created_date",current_timestamp())

# COMMAND ----------

r_df.write.mode("Overwrite").parquet("/mnt/adls27/transform/driver/")

# COMMAND ----------

r_df.write.mode("Overwrite").format("delta").saveAsTable("f1_delta_transform.driver_tb")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/adls27/transformnew/driver_tb/

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/adls27/transform/driver/part-00000-tid-1214210577333222815-b8d82f89-0df5-4f67-9c37-e5434719aa2e-634-1-c000.snappy.parquet"))

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

display(results_join_df.select(col('race_Id'),col('race_name')))

# COMMAND ----------



# COMMAND ----------


