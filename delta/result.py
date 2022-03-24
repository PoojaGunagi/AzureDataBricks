# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location "/mnt/adls27/demo"

# COMMAND ----------

result_df = spark.read.option("inferSchema",True).json("/mnt/adls27/raw/2021-03-21/results.json")

# COMMAND ----------

result_df.write.mode("overwrite").format("delta").saveAsTable("f1_demo.results_tb")

# COMMAND ----------

result_df.write.mode("overwrite").format("delta").save("/mnt/adls27/demo/result_manage_tb/")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.result_manage_table
# MAGIC using delta
# MAGIC location "/mnt/adls27/demo/result_manage_tb"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.result_manage_table

# COMMAND ----------

result_external_df = spark.read.format("delta").load("/mnt/adls27/demo/result_manage_tb")

# COMMAND ----------

display(result_external_df)

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned

# COMMAND ----------


