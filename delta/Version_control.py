# Databricks notebook source
# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge  version as of 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge TIMESTAMP as of  "2022-03-24T11:48:38.000+0000"

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsof","2022-03-24T11:48:38.000+0000").load("/mnt/adls27/demo/drivers_merge/")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false ;
# MAGIC vacuum f1_demo.drivers_merge retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using f1_demo.drivers_merge version as of 8 src
# MAGIC   on (tgt.driverId = src.driverId)
# MAGIC   when not matched then
# MAGIC   insert *

# COMMAND ----------

# MAGIC %sql desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql select * from f1_demo.drivers_merge version as of 8

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_merge where driverId = 1

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction logs

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_txn
# MAGIC (
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createddate date,
# MAGIC updateddate date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId=1

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId=2

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from  f1_demo.drivers_txn
# MAGIC 
# MAGIC where driverId=1

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_convert_to_delta
# MAGIC (
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createddate date,
# MAGIC updateddate date
# MAGIC )
# MAGIC using parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_convert_to_delta
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta f1_demo.drivers_convert_to_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_convert_to_delta

# COMMAND ----------

df=spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/adls27/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/mnt/adls27/demo/drivers_convert_to_delta_new`

# COMMAND ----------


