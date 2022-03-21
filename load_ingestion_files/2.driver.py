# Databricks notebook source
# MAGIC %python
# MAGIC dbutils.widgets.text("p_data_source","")
# MAGIC v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %python
# MAGIC   dbutils.widgets.text("p_file_date","2021-03-21")
# MAGIC v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, StructField,DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename",StringType(),True),StructField("surname",StringType(),True)])

# COMMAND ----------

driver_schema = StructType(fields=[StructField("driverId",IntegerType(),False),StructField("driverRef",StringType(),True),StructField("number",IntegerType(),True),StructField("code",StringType(),True),StructField("name",name_schema),StructField("dob",DateType(),True),StructField("nationality",StringType(),True),StructField("url",StringType(),True)])

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %python
# MAGIC df=spark.read.schema(driver_schema) \
# MAGIC .json(f"/mnt/adls27/raw/{v_file_date}/drivers.json")
# MAGIC display(df)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import *

# COMMAND ----------

driver_rename_df=df.withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("ingestion_date",current_timestamp()).withColumn("name",concat(col('name.forename'),lit(' '),col('name.surname')))

# COMMAND ----------


driver_final_df=driver_rename_df.drop("url")

# COMMAND ----------

final_df=driver_final_df.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

final_df.write.mode("append").format("parquet").saveAsTable("f1_processed_new.driver_tb")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended  f1_processed_new.driver_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(*) from f1_processed_new.driver_tb
# MAGIC group by file_date

# COMMAND ----------


