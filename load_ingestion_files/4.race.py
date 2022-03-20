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

races_schema = StructType(fields=[StructField("raceid",IntegerType(),False),StructField("year",IntegerType(),True),StructField("round",IntegerType(),True),StructField("circuitid",IntegerType(),True),StructField("name",StringType(),True),StructField("date",DateType(),True),StructField("time",StringType(),True),StructField("url",StringType(),True)])

# COMMAND ----------

# MAGIC %python
# MAGIC df=spark.read.option("header",True) \
# MAGIC .schema(races_schema) \
# MAGIC .csv(f"/mnt/adls27/raw/{v_file_date}/races.csv")
# MAGIC display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_race_select=df.select(col("raceid"),col("name"),col("year"),col("round"),col("circuitid"),col("date"),col("time"))
display(df_race_select)

# COMMAND ----------

renamed_df=df_race_select.withColumnRenamed("raceid","race_id") \
.withColumnRenamed("year","race_year") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed("circuitid","circuit_id") \
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

transform_col_df= renamed_df.withColumn("ingested_time",current_timestamp()) \
.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

#%sql drop table f1_processed_new.race_tb

# COMMAND ----------

transform_col_df.write.mode("append").partitionBy('race_year').format("parquet").saveAsTable("f1_processed_new.race_tb")

# COMMAND ----------

# MAGIC %sql select race_id, count(*) from f1_processed_new.race_tb
# MAGIC group by race_id

# COMMAND ----------


