# Databricks notebook source
# MAGIC %python
# MAGIC dbutils.widgets.text("p_data_source","")
# MAGIC v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %python
# MAGIC   dbutils.widgets.text("p_file_date","2021-03-21")
# MAGIC v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.read.json("/mnt/adls27/raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId,count(1)
# MAGIC from results_cutover
# MAGIC group by raceId
# MAGIC order by raceId desc

# COMMAND ----------

spark.read.json("/mnt/adls27/raw/2021-03-28/results.json").createOrReplaceTempView("results_cutover_1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId,count(1)
# MAGIC from results_cutover_1
# MAGIC group by raceId
# MAGIC order by raceId desc

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId,count(1)
# MAGIC from results_cutover_2
# MAGIC group by raceId
# MAGIC order by raceId desc

# COMMAND ----------

spark.read.json("/mnt/adls27/raw/2021-04-18/results.json").createOrReplaceTempView("results_cutover_2")

# COMMAND ----------

result_schema="resultId INT, raceId INT,driverId INT,constructorId INT,number INT,grid INT,position INT,positionText STRING,positionOrder INT,points DOUBLE,laps INT,time STRING,milliseconds INT,fastestLap INT,rank INT,fastestLapTime STRING, fastestLapSpeed String,statusId INT"

# COMMAND ----------

# MAGIC %python
# MAGIC df=spark.read.schema(result_schema) \
# MAGIC .json(f"/mnt/adls27/raw/{v_file_date}/results.json")
# MAGIC display(df)

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

final_df=result_renamed_df.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

final_df.schema.names

# COMMAND ----------

spark.conf.set("spark.sql.source.partitionOverwriteMode","dynamic")

# COMMAND ----------

result_final_df=final_df.select('result_id',
 "race_id",
 'driver_Id',
 'constructor_Id',
 'number',
 'grid',
 'position',
 'position_text',
 'position_order',
 'points',
 'laps',
 'time',
 'milliseconds',
 'fastest_lap',
 'rank',
 'fastest_lap_time',
 'fastest_lap_speed',
 'ingestion_date',
 'data_source',
 'file_date')

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_processed_new
# MAGIC location "/mnt/adls27/processedlatest"

# COMMAND ----------

# MAGIC %sql show databases

# COMMAND ----------

"""
if (spark._jsparkSession.catalog().tableExists("f1_processed_new.result_tb")):
    result_final_df.write.mode("Overwrite").inserInto("f1_processed_new.result_tb")
else:
    result_final_df.write.mode("Overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed_new.result_tb")
  """

# COMMAND ----------

result_final_df.write.mode("Overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed_new.result_tb")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/adls27/processedlatest/result_tb/

# COMMAND ----------

# MAGIC %sql
# MAGIC describe  extended  f1_processed_new.result_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed_new.results_tb
# MAGIC --group by file_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_Id,count(*) from f1_processed_new.results_tb
# MAGIC group by race_Id
# MAGIC order by race_Id desc

# COMMAND ----------


