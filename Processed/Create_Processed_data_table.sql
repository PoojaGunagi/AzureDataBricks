-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
location "/mnt/adls27/processed"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed_new
location "/mnt/adls27/processedlatest"

-- COMMAND ----------

describe database  f1_processed_new

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_transform
location "/mnt/adls27/transform"

-- COMMAND ----------


