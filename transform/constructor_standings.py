# Databricks notebook source
# MAGIC %run "/includes/configuration"

# COMMAND ----------

cons_df = spark.read.parquet("/mnt/adls27/transform/join/")

# COMMAND ----------

display(cons_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

cons_standard_df=cons_df.groupBy("race_year","team").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(cons_standard_df)

# COMMAND ----------

display(driver_standard_df.filter("race_year=2020"))

# COMMAND ----------

from pyspark.sql.functions import desc,rank
from pyspark.sql.window import *

# COMMAND ----------

somethingsomething = Window.partitionBy("race_year").orderBy(desc("total_points"))
    

# COMMAND ----------

r_df =  cons_standard_df.withColumn("rank",rank().over(somethingsomething))

# COMMAND ----------

display(r_df)

# COMMAND ----------

r_df.write.mode("Overwrite").parquet("/mnt/adls27/transform/constructor_standing")

# COMMAND ----------

r_df.write.mode("Overwrite").format("parquet").saveAsTable("f1_transform.constructor_standing_tb")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adls27/transform/constructor_standing/

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/adls27/transform/constructor_standing/part-00000-tid-2439086404043606859-e01d87e8-34f5-49b4-af7b-5fe534e07dfe-679-1-c000.snappy.parquet"))

# COMMAND ----------

display(r_df.filter("race_year=2020"))

# COMMAND ----------

final_df=select_df.withColumn("created_date",current_timestamp())

# COMMAND ----------

r_df.write.mode("Overwrite").parquet(f"{presentation_path}")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/adls27/presentation/

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/adls27/presentation/part-00000-tid-2894271887364369683-53f4534b-d52b-4e3b-802a-5f7ce4544b92-400-1-c000.snappy.parquet"))

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

display(results_join_df.select(col('race_Id'),col('race_name')))

# COMMAND ----------



# COMMAND ----------

