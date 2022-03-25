# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.widgets.text("p_data_source","")
# MAGIC v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %python
# MAGIC   dbutils.widgets.text("p_file_date","2021-03-21")
# MAGIC v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

pit_points_schema = StructType(fields=[StructField("raceId",IntegerType(),False),StructField("driverId",IntegerType(),True),StructField("stop",StringType(),True),StructField("lap",IntegerType(),True),StructField("time",StringType(),True),StructField("duration",StringType(),True),StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

# MAGIC %python
# MAGIC df=spark.read.schema(pit_points_schema) \
# MAGIC .option("multiline",True)\
# MAGIC .json(f"/mnt/adls27/raw/{v_file_date}/pit_stops.json")
# MAGIC display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

final_df=df.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

#df.write.mode("Overwrite").parquet("/mnt/adls27/processed/pit_stop/")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_delta_ingest.pit_stop_tb

# COMMAND ----------

final_df.write.mode("Overwrite").format("delta").saveAsTable("f1_delta_ingest.pit_stop_tb")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),file_date
# MAGIC from f1_delta_ingest.pit_stop_tb
# MAGIC group by file_date

# COMMAND ----------

# MAGIC %fs ls
