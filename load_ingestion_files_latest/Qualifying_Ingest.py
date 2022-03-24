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

qualifying_schema = StructType(fields=[StructField("qualifyId",IntegerType(),False),StructField("raceId",IntegerType(),True),StructField("driverId",IntegerType(),True),StructField("constructorId",IntegerType(),True),StructField("position",IntegerType(),True),StructField("number",IntegerType(),True),StructField("q1",StringType(),True),StructField("q2",StringType(),True),StructField("q3",StringType(),True)])

# COMMAND ----------

# MAGIC %python
# MAGIC df=spark.read.schema(qualifying_schema) \
# MAGIC .option("multiline",True)\
# MAGIC .json(f"/mnt/adls27/raw/{v_file_date}/qualifying/")
# MAGIC display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

final_df=df.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

final_df.write.mode("Overwrite").format("delta").saveAsTable("f1_delta_ingest.qualifying_tb")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),file_date
# MAGIC from f1_delta_ingest.qualifying_tb
# MAGIC group by file_date
