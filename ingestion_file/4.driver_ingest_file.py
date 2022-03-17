# Databricks notebook source
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, StructField,DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename",StringType(),True),StructField("surname",StringType(),True)])

# COMMAND ----------

driver_schema = StructType(fields=[StructField("driverId",IntegerType(),False),StructField("driverRef",StringType(),True),StructField("number",IntegerType(),True),StructField("code",StringType(),True),StructField("name",name_schema),StructField("dob",DateType(),True),StructField("nationality",StringType(),True),StructField("url",StringType(),True)])

# COMMAND ----------

driver_read_df=spark.read.schema(driver_schema).json("/mnt/adls27/raw/drivers.json")

# COMMAND ----------

display(driver_read_df)

# COMMAND ----------

driver_read_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

driver_rename_df=driver_read_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("ingestion_date",current_timestamp()).withColumn("name",concat(col('name.forename'),lit(' '),col('name.surname')))

# COMMAND ----------

display(driver_rename_df)

# COMMAND ----------

driver_final_df=driver_rename_df.drop("url")

# COMMAND ----------

display(driver_final_df)

# COMMAND ----------

driver_final_df.write.mode("overwrite").parquet("/mnt/adls27/processed/driver")

# COMMAND ----------

driver_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.driver_tb")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adls27/processed/driver

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/adls27/processed/driver/part-00000-tid-6875479266951548850-c075a644-41e3-43a5-8081-92097520f73f-12-1-c000.snappy.parquet"))

# COMMAND ----------

