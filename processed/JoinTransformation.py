# Databricks notebook source
# MAGIC %run "/includes/configuration"

# COMMAND ----------

presentation_path

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_path}/races")

# COMMAND ----------

renamed_race_df=races_df.withColumnRenamed("race_id","r_race_id").withColumnRenamed("race_year","r_race_year")

# COMMAND ----------

display(renamed_race_df)

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_path}/circuits")

# COMMAND ----------



# COMMAND ----------

display(circuits_df)

# COMMAND ----------

result_df=spark.read.parquet(f"{processed_path}/results")

# COMMAND ----------

display(result_df)

# COMMAND ----------

result_renamed_df=result_df.withColumnRenamed("result_df","re_result_df")

# COMMAND ----------

driver_df=spark.read.parquet(f"{processed_path}/driver")

# COMMAND ----------

display(driver_df)

# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_path}/constructors")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

races_circuit_join_df=circuits_df.join(renamed_race_df,circuits_df.circuit_id==renamed_race_df.circuit_id,"inner")

# COMMAND ----------

display(races_circuit_join_df)

# COMMAND ----------

display(result_renamed_df)

# COMMAND ----------

join_df=result_renamed_df.join(renamed_race_df,renamed_race_df.r_race_id==result_renamed_df.race_Id,"inner")\
.join(driver_df,driver_df.driver_id==result_renamed_df.driver_Id,"inner")\
.join(constructors_df,constructors_df.constructor_id==result_renamed_df.constructor_Id,"inner")


# COMMAND ----------

display(join_df)

# COMMAND ----------



# COMMAND ----------

final_join_df=join_df.join(races_circuit_join_df,races_circuit_join_df.r_race_id==join_df.r_race_id)

# COMMAND ----------

display(final_join_df)

# COMMAND ----------

select_col_df=final_join_df.select(col('r_race_year'),col('race_name'),col('race_date'),col('circuit_location'),col('driver_name'),col('driver_nationality'),col('driver_number'),col('team'),col('grid'),col('fastest_lap'),col('race_time'),col('points')).withColumn("created_date",current_timestamp())

# COMMAND ----------

circuit joins race on circuit_id join - result
join result table based on race id
