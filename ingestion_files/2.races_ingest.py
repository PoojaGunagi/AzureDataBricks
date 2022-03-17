# Databricks notebook source
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, StructField,DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceid",IntegerType(),False),StructField("year",IntegerType(),True),StructField("round",IntegerType(),True),StructField("circuitid",IntegerType(),True),StructField("name",StringType(),True),StructField("date",DateType(),True),StructField("time",StringType(),True),StructField("url",StringType(),True)])

# COMMAND ----------

df=spark.read.option("header",True) \
.schema(races_schema) \
.csv("/mnt/adls27/raw/races.csv")
display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_race_select=df.select(col("raceid"),col("name"),col("year"),col("round"),col("circuitid"),col("date"),col("time"))
display(df_race_select)

# COMMAND ----------

renamed_df=df_race_select.withColumnRenamed("raceid","race_id") \
.withColumnRenamed("year","race_year") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed("circuitid","circuit_id") 

# COMMAND ----------

display(renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

transform_col_df= renamed_df.withColumn("ingested_time",current_timestamp()) \
.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(transform_col_df)

# COMMAND ----------

transform_col_df.write.mode("overwrite").partitionBy('race_year').parquet("/mnt/adls27/processed/races")

# COMMAND ----------

transform_col_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.race_tb")

# COMMAND ----------

# MAGIC %sql select * from f1_processed.race_tb

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adls27/processed/races/race_year=1957/

# COMMAND ----------

df=spark.read.format('parquet').load("dbfs:/mnt/adls27/processed/races/race_year=1957/part-00000-tid-296209852204845026-eca96354-46c5-4974-a205-055fbb390a48-39-8.c000.snappy.parquet")
display(df)

# COMMAND ----------

