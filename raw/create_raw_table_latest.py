# Databricks notebook source
# MAGIC %sql
# MAGIC  create database f1_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if  EXISTS f1_raw.circuits_tb;
# MAGIC create table if not exists f1_raw.circuits_tb(
# MAGIC circuitId INT,
# MAGIC circuitRef STRING,
# MAGIC name STRING,
# MAGIC location STRING,
# MAGIC country DOUBLE,
# MAGIC lat DOUBLE,
# MAGIC lng DOUBLE,
# MAGIC alt INT,
# MAGIC url STRING
# MAGIC )
# MAGIC USING csv 
# MAGIC OPTIONS (path "/mnt/adls27/raw/circuits.csv", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.circuits_tb;

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if  EXISTS f1_raw.race_tb;
# MAGIC create table if not exists f1_raw.race_tb(
# MAGIC raceid INT,
# MAGIC year INT,
# MAGIC round INT,
# MAGIC circuitid INT,
# MAGIC name STRING,
# MAGIC date DATE ,
# MAGIC time STRING,
# MAGIC url STRING
# MAGIC )
# MAGIC USING csv 
# MAGIC OPTIONS (path "/mnt/adls27/raw/races.csv", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.race_tb

# COMMAND ----------

# MAGIC %md
# MAGIC create table for JSON files

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if  EXISTS f1_raw.constructor_tb;
# MAGIC create table f1_raw.constructor_tb
# MAGIC (
# MAGIC constructorId INT, 
# MAGIC constructorRef STRING,
# MAGIC name STRING,
# MAGIC nationality STRING,
# MAGIC url STRING
# MAGIC )
# MAGIC USING json 
# MAGIC OPTIONS (path "/mnt/adls27/raw/constructors.json", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.constructor_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if  EXISTS f1_raw.driver_tb;
# MAGIC create table f1_raw.driver_tb
# MAGIC (
# MAGIC driverId INT, 
# MAGIC driverRef STRING,
# MAGIC number INT,
# MAGIC code STRING,
# MAGIC name STRUCT<forename:STRING,surname:STRING>,
# MAGIC dob DATE,
# MAGIC nationality STRING,
# MAGIC url STRING
# MAGIC )
# MAGIC USING json 
# MAGIC OPTIONS (path "/mnt/adls27/raw/drivers.json", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.driver_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if  EXISTS f1_raw.result_tb;
# MAGIC create table f1_raw.result_tb
# MAGIC (
# MAGIC resultId INT, raceId INT,driverId INT,constructorId INT,number INT,grid INT,position INT,positionText STRING,positionOrder INT,points DOUBLE,laps INT,time STRING,milliseconds INT,fastestLap INT,rank INT,fastestLapTime STRING, fastestLapSpeed String,statusId INT
# MAGIC )
# MAGIC USING json 
# MAGIC OPTIONS (path "/mnt/adls27/raw/results.json", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.result_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if  EXISTS f1_raw.pits_stops_tb;
# MAGIC create table f1_raw.pits_stops_tb
# MAGIC (
# MAGIC driverId INT, duration STRING,lap INT,milliseconds INT,raceId INT,stop INT,time STRING
# MAGIC )
# MAGIC USING json 
# MAGIC OPTIONS (path "/mnt/adls27/raw/pit_stops.json",multiline true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.pits_stops_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if  EXISTS f1_raw.lap_time_tb;
# MAGIC create table f1_raw.lap_time_tb
# MAGIC (
# MAGIC raceId INT, 
# MAGIC driverId INT,
# MAGIC lap INT,
# MAGIC position INT,
# MAGIC time STRING,
# MAGIC milliseconds int
# MAGIC )
# MAGIC USING csv 
# MAGIC OPTIONS (path "/mnt/adls27/raw/lap_times/", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_raw.lap_time_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if  EXISTS f1_raw.qualify_tb;
# MAGIC create table f1_raw.qualify_tb
# MAGIC (
# MAGIC qualifyId int,
# MAGIC raceId int,
# MAGIC driverId int,
# MAGIC constructorId int,
# MAGIC number int,position int,
# MAGIC q1 string,
# MAGIC q2 string,
# MAGIC q3 string
# MAGIC )
# MAGIC USING json 
# MAGIC OPTIONS (path "/mnt/adls27/raw/qualifying/", multiline true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_raw.qualify_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended f1_raw.qualify_tb

# COMMAND ----------

