# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, StructField

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(),False),StructField("circuitRef",StringType(),True),StructField("name",StringType(),True),StructField("location",StringType(),True),StructField("country",StringType(),True),StructField("lat",DoubleType(),True),StructField("lng",DoubleType(),True),StructField("alt",IntegerType(),True),StructField("url",StringType(),True)])

# COMMAND ----------

df=spark.read.option("header",True) \
.schema(circuits_schema) \
.csv("/mnt/adls27/raw/circuits.csv")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_select=df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))
display(df_select)

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming columns

# COMMAND ----------

renamed_df=df_select.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","lattitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude") 

# COMMAND ----------

display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC adding ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df= renamed_df.withColumn("ingested_time",current_timestamp()).withColumn("env",lit("Production"))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC write data to ADLs as parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC use f1_processed

# COMMAND ----------


final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuit_tb")

# COMMAND ----------

#final_df.write.mode("overwrite").parquet("/mnt/adls27/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adls27/processed/circuits

# COMMAND ----------

df=spark.read.format("parquet").load("dbfs:/mnt/adls27/processed/circuits/part-00000-tid-7416718176354393534-c8a27f0e-92be-4e92-8dc3-d20bf4cc468c-12-1-c000.snappy.parquet")
display(df)

# COMMAND ----------


