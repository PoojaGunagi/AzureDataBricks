-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
location "/mnt/adls27/processed"

-- COMMAND ----------

desc database f1_processed

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_transform
location "/mnt/adls27/transform"

-- COMMAND ----------

