# Databricks notebook source
# MAGIC %sql
# MAGIC select * from f1_demo.result_manage_table

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.result_manage_table
# MAGIC SET points = 11- position
# MAGIC where position <=10

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable =  DeltaTable.forPath(spark,"/mnt/adls27/demo/result_manage_tb/")

deltaTable.update("position <= 10",{"points":"'21-position'"})




# COMMAND ----------

from delta.tables import *

deltaTable =  DeltaTable.forPath(spark,"/mnt/adls27/demo/result_manage_tb/")


deltaTable.delete("points = 0")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.result_manage_table

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema",True) \
.json("/mnt/adls27/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId","dob","name.forename","name.surname")


# COMMAND ----------

drivers_day1_df.createTempView("driver_day1_tb")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema",True) \
.json("/mnt/adls27/raw/2021-03-28/drivers.json") \
.filter("driverId between 6 and 15") \
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))



# COMMAND ----------

drivers_day2_df.createTempView("drivers_day2_tb")

# COMMAND ----------

display( drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema",True) \
.json("/mnt/adls27/raw/2021-03-28/drivers.json") \
.filter("driverId between 1 and 5 or driverId between 16 and 20") \
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))


# COMMAND ----------

drivers_day3_df.createTempView("drivers_day3_tb")

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_merge
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
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using driver_day1_tb upd
# MAGIC on tgt.driverId=upd.driverId
# MAGIC when matched then
# MAGIC   update set tgt.dob=upd.dob,
# MAGIC   tgt.forename=upd.forename,
# MAGIC   tgt.surname=upd.surname,
# MAGIC   tgt.updateddate=current_timestamp
# MAGIC   when not matched then
# MAGIC   insert (driverId,dob,forename,surname,createdDate) values(driverId,dob,forename,surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using drivers_day2_tb upd
# MAGIC on tgt.driverId=upd.driverId
# MAGIC when matched then
# MAGIC   update set tgt.dob=upd.dob,
# MAGIC   tgt.forename=upd.forename,
# MAGIC   tgt.surname=upd.surname,
# MAGIC   tgt.updateddate=current_timestamp
# MAGIC   when not matched then
# MAGIC   insert (driverId,dob,forename,surname,createdDate) values(driverId,dob,forename,surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using drivers_day3_tb upd
# MAGIC on tgt.driverId=upd.driverId
# MAGIC when matched then
# MAGIC   update set tgt.dob=upd.dob,
# MAGIC   tgt.forename=upd.forename,
# MAGIC   tgt.surname=upd.surname,
# MAGIC   tgt.updateddate=current_timestamp
# MAGIC   when not matched then
# MAGIC   insert (driverId,dob,forename,surname,createdDate) values(driverId,dob,forename,surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql select * from f1_demo.drivers_merge

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *
deltaTable = DeltaTable.forPath(spark,"/mnt/adls27/demo/drivers_merge")
deltaTable.alias("tgt").merge(drivers_day3_df.alias("upd"),"tgt.driverId=upd.driverId") \
.whenMatchedUpdate(set = {"dob":"upd.dob","forename":"upd.forename","surname":"upd.surname","updateddate":"current_timestamp()" } ) \
.whenNotMatchedInsert(values ={ "driverId": "upd.driverId", "dob": "upd.dob", "forename":"upd.forename","surname":"upd.surname",
                      "createdDate":"current_timestamp()"}).execute()

# COMMAND ----------

# MAGIC %sql select * from f1_demo.drivers_merge

# COMMAND ----------


