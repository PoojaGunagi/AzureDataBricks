# Databricks notebook source
result_schema="resultId INT, raceId INT,driverId INT,constructorId INT,number INT,grid INT,position INT,positionText STRING,positionOrder INT,points DOUBLE,laps INT,time STRING,milliseconds INT,fastestLap INT,rank INT,fastestLapTime STRING, fastestLapSpeed String,statusId INT"

# COMMAND ----------

df=spark.read.option("header",True) \
.schema(result_schema) \
.json("/mnt/adls27/raw/results.json")
display(df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

result_renamed_df=df.withColumnRenamed("resultId","result_id") \
.withColumnRenamed("driverId","driver_Id") \
.withColumnRenamed("constructorId","constructor_Id") \
.withColumnRenamed("raceId","race_Id") \
.withColumnRenamed("positionText","position_text") \
.withColumnRenamed("positionOrder","position_order") \
.withColumnRenamed("fastestLap","fastest_lap") \
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
.withColumnRenamed("fastestLapTime","fastest_lap_time") \
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(result_renamed_df)

# COMMAND ----------

fina_result_df = result_renamed_df.drop("statusId")

# COMMAND ----------

display(fina_result_df)

# COMMAND ----------

fina_result_df.write.mode("Overwrite").partitionBy("race_id").parquet("/mnt/adls27/processed/results")

# COMMAND ----------

fina_result_df.write.mode("Overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.result_tb")