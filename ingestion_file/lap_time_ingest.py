# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

pit_points_schema = StructType(fields=[StructField("raceId",IntegerType(),False),StructField("driverId",IntegerType(),True),StructField("lap",IntegerType(),True),StructField("position",IntegerType(),True),StructField("time",StringType(),True),StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

df=spark.read.schema(pit_points_schema) \
.csv("/mnt/adls27/raw/lap_times")
display(df)

# COMMAND ----------

#df.write.mode("Overwrite").parquet("/mnt/adls27/processed/lap_time/")

# COMMAND ----------

df.write.mode("Overwrite").format("parquet").saveAsTable("f1_processed.lap_time_tb")

# COMMAND ----------

