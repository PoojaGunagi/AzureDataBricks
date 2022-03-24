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

constructor_schema="constructorId INT, constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %python
# MAGIC df=spark.read.schema(constructor_schema) \
# MAGIC .json(f"/mnt/adls27/raw/{v_file_date}/constructors.json")
# MAGIC display(df)

# COMMAND ----------

constructor_drop_df=df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import  *

# COMMAND ----------

constructor_final_df=constructor_drop_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("ingestion_date",current_timestamp()).withColumnRenamed("name","team").withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

constructor_final_df.write.mode("Overwrite").format("delta").saveAsTable("f1_delta_ingest.constructor_tb")

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, count(*) from f1_delta_ingest.constructor_tb
# MAGIC group by file_date

# COMMAND ----------


