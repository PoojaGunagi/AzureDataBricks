# Databricks notebook source
constructor_schema="constructorId INT, constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adls27/raw/constructors.json

# COMMAND ----------

constructor_df=spark.read.schema(constructor_schema).json("/mnt/adls27/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC drop unwanted column

# COMMAND ----------

constructor_drop_df=constructor_df.drop('url')

# COMMAND ----------

display(constructor_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC rename column and ingest new col

# COMMAND ----------

from pyspark.sql.functions import  *

# COMMAND ----------

constructor_final_df=constructor_drop_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("ingestion_date",current_timestamp()).withColumnRenamed("name","team")

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC md
# MAGIC   write data to parquet

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructor_tb")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructor_tb

# COMMAND ----------

#constructor_final_df.write.mode("overwrite").parquet("/mnt/adls27/processed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adls27/processed/constructors

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/adls27/processed/constructors/part-00000-tid-9074584390407849534-a375bb40-9d31-4ac0-9b35-7e3081d94c04-11-1-c000.snappy.parquet"))

# COMMAND ----------

