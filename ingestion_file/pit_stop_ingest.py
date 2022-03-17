# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adls27/raw/pit_stops.json

# COMMAND ----------

pit_points_schema = StructType(fields=[StructField("raceId",IntegerType(),False),StructField("driverId",IntegerType(),True),StructField("stop",StringType(),True),StructField("lap",IntegerType(),True),StructField("time",StringType(),True),StructField("duration",StringType(),True),StructField("milliseconds",StringType(),True)])

# COMMAND ----------

df=spark.read.schema(pit_points_schema) \
.option("multiline",True)\
.json("/mnt/adls27/raw/pit_stops.json")
display(df)

# COMMAND ----------

#df.write.mode("Overwrite").parquet("/mnt/adls27/processed/pit_stop/")

# COMMAND ----------

df.write.mode("Overwrite").format("parquet").saveAsTable("f1_processed.pit_stop_tb")

# COMMAND ----------

