# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId",IntegerType(),False),StructField("raceId",IntegerType(),True),StructField("driverId",IntegerType(),True),StructField("constructorId",IntegerType(),True),StructField("position",IntegerType(),True),StructField("number",IntegerType(),True),StructField("q1",StringType(),True),StructField("q2",StringType(),True),StructField("q3",StringType(),True)])

# COMMAND ----------

qualify_df=spark.read.schema(qualifying_schema) \
.option("multiline",True)\
.json("/mnt/adls27/raw/qualifying/")
display(qualify_df)
qualify_df.count()

# COMMAND ----------

qualify_df.write.mode("Overwrite").parquet("/mnt/adls27/processed/qualifying/")

# COMMAND ----------

qualify_df.write.mode("Overwrite").format("parquet").saveAsTable("f1_processed.qualifying_tb")