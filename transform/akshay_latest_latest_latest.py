# Databricks notebook source
# MAGIC %run "/includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet("/mnt/adls27/processed//races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

renamed_race_df=races_df.withColumnRenamed("date","race_date")

# COMMAND ----------

display(renamed_race_df)

# COMMAND ----------

circuits_df = spark.read.parquet("dbfs:/mnt/adls27/processed/circuits/")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_renamed_df=circuits_df.withColumnRenamed("location","circuit_location")

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

driver_df = spark.read.parquet("dbfs:/mnt/adls27/processed/driver/")

# COMMAND ----------

display(driver_df)

# COMMAND ----------

driver_renamed_df=driver_df.withColumnRenamed("name","driver_name").withColumnRenamed("number","driver_number").withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

display(driver_renamed_df)

# COMMAND ----------

constructors_df = spark.read.parquet("dbfs:/mnt/adls27/processed/constructors")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

results_df = spark.read.parquet("/mnt/adls27/processed//results")

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


results_join_df = result_renamed_df.join(jo,jo.race_id==result_renamed_df.race_id,"inner")\
.join(driver_renamed_df,driver_renamed_df.driver_id==result_renamed_df.driver_Id,"inner")\
.join(constructors_df,constructors_df.constructor_id==result_renamed_df.constructor_Id,"inner").drop(renamed_race_df.race_id).drop(renamed_race_df.time)

# COMMAND ----------

display(results_join_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

select_df = results_join_df.select(col('race_year'),col('race_name'),col('race_date'),col('circuit_location'),col('driver_name'),col('driver_number'),col('driver_nationality'),col('grid'),col('fastest_lap'),col('race_time'),col('points'),col("position"),col("team")).withColumn("current_date",current_timestamp())

# COMMAND ----------

display(select_df)

# COMMAND ----------

select_df.write.mode("Overwrite").parquet("/mnt/adls27/transform/join/")

# COMMAND ----------

select_df.write.mode("Overwrite").format("parquet").saveAsTable("f1_transform.join_result_tb")

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

# MAGIC %fs
# MAGIC ls dbfs:/mnt/adls27/transform/driver/

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/adls27/transform/driver/part-00000-tid-1214210577333222815-b8d82f89-0df5-4f67-9c37-e5434719aa2e-634-1-c000.snappy.parquet"))

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

display(results_join_df.select(col('race_Id'),col('race_name')))

# COMMAND ----------



# COMMAND ----------

