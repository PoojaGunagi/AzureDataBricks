-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_delta_ingest
location "/mnt/adls27/delta"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("p_data_source","")
-- MAGIC v_data_source=dbutils.widgets.get("p_data_source")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC   dbutils.widgets.text("p_file_date","2021-03-21")
-- MAGIC v_file_date=dbutils.widgets.get("p_file_date")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(),False),StructField("circuitRef",StringType(),True),StructField("name",StringType(),True),StructField("location",StringType(),True),StructField("country",StringType(),True),StructField("lat",DoubleType(),True),StructField("lng",DoubleType(),True),StructField("alt",IntegerType(),True),StructField("url",StringType(),True)])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.option("header",True) \
-- MAGIC .schema(circuits_schema) \
-- MAGIC .csv(f"/mnt/adls27/raw/{v_file_date}/circuits.csv")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_select=df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))
-- MAGIC display(df_select)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC renamed_df=df_select.withColumnRenamed("circuitId","circuit_id") \
-- MAGIC .withColumnRenamed("circuitRef","circuit_ref") \
-- MAGIC .withColumnRenamed("lat","lattitude") \
-- MAGIC .withColumnRenamed("lng","longitude") \
-- MAGIC .withColumnRenamed("alt","altitude") \
-- MAGIC .withColumn("data_source",lit(v_data_source))\
-- MAGIC .withColumn("file_date",lit(v_file_date))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC final_df= renamed_df.withColumn("ingested_time",current_timestamp()).withColumn("env",lit("Production"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(final_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC final_df.write.mode("overwrite").format("delta").saveAsTable("f1_delta_ingest.circuit_tb")

-- COMMAND ----------

select * from f1_delta_ingest.circuit_tb

-- COMMAND ----------

select file_date, count(*) from f1_delta_ingest.circuit_tb
group by file_date

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --delete from parquet.`f1_processed_new.circuit_tb` where file_date='2021-03-21'

-- COMMAND ----------


